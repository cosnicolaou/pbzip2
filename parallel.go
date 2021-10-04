// Copyright 2020 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package pbzip2

import (
	"container/heap"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cosnicolaou/pbzip2/internal/bzip2"
)

var numDecompressionGoRoutines int64

func updateStreamCRC(streamCRC, blockCRC uint32) uint32 {
	return (streamCRC<<1 | streamCRC>>31) ^ blockCRC
}

type decompressorOpts struct {
	verbose     bool
	concurrency int
	progressCh  chan<- Progress
}

type DecompressorOption func(*decompressorOpts)

// BZVerbose controls verbose logging for decompression,
func BZVerbose(v bool) DecompressorOption {
	return func(o *decompressorOpts) {
		o.verbose = v
	}
}

// BZConcurrency sets the degree of concurrency to use, that is,
// the number of threads used for decompression.
func BZConcurrency(n int) DecompressorOption {
	return func(o *decompressorOpts) {
		o.concurrency = n
	}
}

// BZSendUpdates sets the channel for sending progress updates over.
func BZSendUpdates(ch chan<- Progress) DecompressorOption {
	return func(o *decompressorOpts) {
		o.progressCh = ch
	}
}

// Decompressor represents a concurrent decompressor for pbzip streams. The
// decompressor is designed to work in conjunction with Scanner and its
// Decompress method must be called with the values returned by the scanner's
// Block method. Each block is then decompressed in parallel and reassembled
// in the original order.
type Decompressor struct {
	ctx        context.Context
	workWg     sync.WaitGroup
	doneWg     sync.WaitGroup
	workCh     chan *blockDesc
	doneCh     chan *blockDesc
	progressCh chan<- Progress
	prd        *io.PipeReader
	pwr        *io.PipeWriter
	order      uint64

	heap      *blockHeap
	streamCRC uint32
	verbose   bool
}

// Progress is used to report the progress of decompression. Each report pertains
// to a correctly ordered decompression event.
type Progress struct {
	Duration         time.Duration
	Block            uint64
	CRC              uint32
	Compressed, Size int
}

// NewDecompressor creates a new parallel decompressor.
func NewDecompressor(ctx context.Context, opts ...DecompressorOption) *Decompressor {
	o := decompressorOpts{
		concurrency: runtime.GOMAXPROCS(-1),
	}
	for _, fn := range opts {
		fn(&o)
	}
	dc := &Decompressor{
		ctx:        ctx,
		doneCh:     make(chan *blockDesc, o.concurrency),
		workCh:     make(chan *blockDesc, o.concurrency),
		progressCh: o.progressCh,
		heap:       &blockHeap{},
	}
	dc.prd, dc.pwr = io.Pipe()
	heap.Init(dc.heap)
	dc.workWg.Add(o.concurrency)
	dc.doneWg.Add(1)
	for i := 0; i < o.concurrency; i++ {
		go func() {
			atomic.AddInt64(&numDecompressionGoRoutines, 1)
			dc.worker(ctx, dc.workCh, dc.doneCh)
			dc.workWg.Done()
			atomic.AddInt64(&numDecompressionGoRoutines, -1)
		}()
	}
	go func() {
		atomic.AddInt64(&numDecompressionGoRoutines, 1)
		dc.assemble(ctx, dc.doneCh)
		dc.doneWg.Done()
		atomic.AddInt64(&numDecompressionGoRoutines, -1)
	}()
	return dc
}

type blockDesc struct {
	order         uint64
	crc           uint32
	bzipBlockSize int
	block         []byte
	blockSizeBits int
	offset        int

	err      error
	data     []byte
	duration time.Duration
}

func (b *blockDesc) String() string {
	if b == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v: crc %v, size %v, offset %v", b.order, b.crc, len(b.block), b.offset)
}

func (dc *Decompressor) trace(format string, args ...interface{}) {
	if dc.verbose {
		log.Printf(format, args...)
	}
}

func (block *blockDesc) decompress() {
	start := time.Now()
	rd := bzip2.NewBlockReader(block.bzipBlockSize, block.block, block.offset)
	block.data, block.err = ioutil.ReadAll(rd)
	block.duration = time.Since(start)
}

func (dc *Decompressor) worker(ctx context.Context, in <-chan *blockDesc, out chan<- *blockDesc) {
	for {
		select {
		case block := <-in:
			if block == nil {
				return
			}
			dc.trace("decompressing: %s", block)
			block.decompress()
			dc.trace("decompressed: %s, ch %v/%v", block, len(out), cap(out))
			select {
			case out <- block:
			case <-ctx.Done():
			}
		case <-ctx.Done():
			return
		}
	}
}

// Decompress is called for each block to be decompressed.
func (dc *Decompressor) Decompress(bzipBlockSize int, block []byte, offset int, sizeInBits int, crc uint32) error {
	order := atomic.AddUint64(&dc.order, 1)
	select {
	case dc.workCh <- &blockDesc{
		order:         order,
		crc:           crc,
		block:         block,
		blockSizeBits: sizeInBits,
		bzipBlockSize: bzipBlockSize,
		offset:        offset,
	}:
	case <-dc.ctx.Done():
		return dc.ctx.Err()
	}
	return nil
}

// Cancel can be called to unblock any readers that are reading from
// this decompressor and/or the Finish method.
func (dc *Decompressor) Cancel(err error) {
	dc.pwr.CloseWithError(err)
}

// Finish must be called to wait for all of the currently outstanding
// decompression processes to finish and their output to be reassembled.
// It should be called exactly once.
func (dc *Decompressor) Finish() (crc uint32, err error) {
	select {
	case <-dc.ctx.Done():
		err = dc.ctx.Err()
	default:
	}
	close(dc.workCh)
	dc.workWg.Wait()
	close(dc.doneCh)
	dc.doneWg.Wait()
	crc = dc.streamCRC
	return
}

type blockHeap []*blockDesc

func (h blockHeap) Len() int           { return len(h) }
func (h blockHeap) Less(i, j int) bool { return h[i].order < h[j].order }
func (h blockHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *blockHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*blockDesc))
}

func (h *blockHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (dc *Decompressor) tryMergeBlocks(ctx context.Context, ch <-chan *blockDesc, min *blockDesc) bool {
	for len(*dc.heap) <= 1 {
		select {
		case block := <-ch:
			if block == nil {
				// channel has been closed.
				return false
			}
			heap.Push(dc.heap, block)
		case <-ctx.Done():
			err := ctx.Err()
			dc.trace("tryMergeBlocks: %v", err)
			dc.pwr.CloseWithError(err)
			return false
		}
	}
	fmt.Printf("merging: %v\n", min)
	if len(*dc.heap) > 1 {
		next := (*dc.heap)[1]
		merged := &blockDesc{
			order: min.order,
			crc:   min.crc,
			blockSizeBits: min.blockSizeBits +
				(len(blockMagic) * 8) +
				next.blockSizeBits,
			bzipBlockSize: min.bzipBlockSize,
			offset:        min.offset,
		}
		m := make([]byte, 0, len(min.block)+len(next.block)+len(blockMagic))

		m = append(m, min.block...)
		m = append(m, blockMagic[:]...)
		m = append(m, next.block...)

		merged.block = m
		fmt.Printf("NEW: L: %v\n", len(m))
		merged.decompress()
		if merged.err != nil {
			fmt.Printf("WTF..... %v\n", merged.err)
			return false
		}
		fmt.Printf("ELSE..... all good...\n")

		// merge succeeded, remove the block that was merged into the
		// top of the heap from the heap.
		(*dc.heap)[0] = merged
		heap.Remove(dc.heap, 1)
		return true
	}
	return false
}

func (dc *Decompressor) assemble(ctx context.Context, ch <-chan *blockDesc) {
	defer dc.pwr.Close()
	expected := uint64(1)
	for {
		dc.trace("assemble select")
		select {
		case block := <-ch:
			dc.trace("assemble: %v", block)
			if block != nil {
				heap.Push(dc.heap, block)
			}
			for len(*dc.heap) > 0 {
				min := (*dc.heap)[0]
				if min.order != expected {
					break
				}
				if err := min.err; err != nil {
					if !dc.tryMergeBlocks(ctx, ch, min) {
						dc.pwr.CloseWithError(err)
						return
					}
					min = (*dc.heap)[0]
				}
				if _, err := dc.pwr.Write(min.data); err != nil {
					dc.pwr.CloseWithError(err)
					return
				}
				dc.streamCRC = updateStreamCRC(dc.streamCRC, min.crc)
				heap.Remove(dc.heap, 0)
				if dc.progressCh != nil {
					dc.progressCh <- Progress{
						Duration:   min.duration,
						Block:      min.order,
						CRC:        min.crc,
						Compressed: len(min.block),
						Size:       len(min.data),
					}
				}
				expected++
			}
			if block == nil && len(*dc.heap) == 0 {
				return
			}
		case <-ctx.Done():
			err := ctx.Err()
			dc.trace("assemble: %v", err)
			dc.pwr.CloseWithError(err)
			return
		}
	}
}

// Read implements io.Reader on the decompressed stream.
func (dc *Decompressor) Read(buf []byte) (int, error) {
	return dc.prd.Read(buf)
}
