package pbzip2

import (
	"container/heap"
	"io"
	"io/ioutil"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cosnicolaou/pbzip2/bzip2"
)

func updateStreamCRC(streamCRC, blockCRC uint32) uint32 {
	return (streamCRC<<1 | streamCRC>>31) ^ blockCRC
}

type Decompressor struct {
	workWg     sync.WaitGroup
	doneWg     sync.WaitGroup
	workCh     chan *blockDesc
	doneCh     chan *blockDesc
	progressCh chan Progress
	prd        *io.PipeReader
	pwr        *io.PipeWriter
	order      uint64
	heapMu     sync.Mutex
	heap       *blockHeap // GUARDED_BY(heapMu)
	streamCRC  uint32
}

type Progress struct {
	Duration time.Duration
	Block    uint64
	CRC      uint32
	Size     int
}

func NewDecompressor(concurrency int, progressCh chan Progress) *Decompressor {
	dc := &Decompressor{
		doneCh:     make(chan *blockDesc, concurrency),
		workCh:     make(chan *blockDesc, concurrency),
		progressCh: progressCh,
		heap:       &blockHeap{},
	}
	dc.prd, dc.pwr = io.Pipe()
	heap.Init(dc.heap)
	dc.workWg.Add(concurrency)
	dc.doneWg.Add(1)
	for i := 0; i < concurrency; i++ {
		go func() {
			worker(dc.workCh, dc.doneCh)
			dc.workWg.Done()
		}()
	}
	go func() {
		dc.assemble()
		dc.doneWg.Done()
	}()
	return dc
}

type blockDesc struct {
	order     uint64
	crc       uint32
	blockSize int
	block     []byte
	offset    int

	err      error
	data     []byte
	duration time.Duration
}

func worker(in, out chan *blockDesc) {
	for block := range in {
		start := time.Now()
		rd := bzip2.NewBlockReader(block.blockSize, block.block, block.offset)
		block.data, block.err = ioutil.ReadAll(rd)
		block.duration = time.Since(start)
		out <- block
	}
}

func (dc *Decompressor) NewBlock(blockSize int, block []byte, offset int, crc uint32) {
	order := atomic.AddUint64(&dc.order, 1)
	dc.workCh <- &blockDesc{
		order:     order,
		crc:       crc,
		block:     block,
		blockSize: blockSize,
		offset:    offset,
	}
}

func (dc *Decompressor) Finish() uint32 {
	close(dc.workCh)
	dc.workWg.Wait()
	close(dc.doneCh)
	dc.doneWg.Wait()
	return dc.streamCRC
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

func (dc *Decompressor) assemble() {
	expected := uint64(1)
	for block := range dc.doneCh {
		dc.heapMu.Lock()
		heap.Push(dc.heap, block)
		for len(*dc.heap) > 0 {
			min := (*dc.heap)[0]
			if min.order != expected {
				break
			}
			if err := min.err; err != nil {
				dc.pwr.CloseWithError(err)
			}
			if _, err := dc.pwr.Write(min.data); err != nil {
				dc.pwr.CloseWithError(err)
			}
			dc.streamCRC = updateStreamCRC(dc.streamCRC, min.crc)
			heap.Remove(dc.heap, 0)
			if dc.progressCh != nil {
				dc.progressCh <- Progress{
					Duration: min.duration,
					Block:    min.order,
					CRC:      min.crc,
					Size:     len(min.data),
				}
			}
			expected++
		}
		dc.heapMu.Unlock()
	}
	dc.pwr.Close()
}

func (dc *Decompressor) Read(buf []byte) (int, error) {
	return dc.prd.Read(buf)
}
