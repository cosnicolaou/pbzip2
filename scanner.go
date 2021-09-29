// Copyright 2019 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.
package pbzip2

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

type scannerOpts struct {
	maxPreamble int
}

// ScannerOption represenst an option to NewBZ2BlockScanner.
type ScannerOption func(*scannerOpts)

// ScanBlockOverhead sets the size of the overhead, in bytes, that
// the scanner assumes is sufficient to capture all of the bzip2 per block
// data structures. It should only ever be needed if the scanner is unable to
// find a magic number.
func ScanBlockOverhead(b int) ScannerOption {
	return func(o *scannerOpts) {
		o.maxPreamble = b
	}
}

// See https://en.wikipedia.org/wiki/Bzip2 for an explanation of the file
// format.
var (
	bzip2FileMagic = []byte{0x42, 0x5a} // "BZ"

	bzip2BlockMagic = [6]byte{0x31, 0x41, 0x59, 0x26, 0x53, 0x59}
	bzip2EOSMagic   = [6]byte{0x17, 0x72, 0x45, 0x38, 0x50, 0x90}

	firstBlockMagicLookup, secondBlockMagicLookup map[uint32]uint8
	initOnce                                      sync.Once
)

func Init() {
	initOnce.Do(func() {
		firstBlockMagicLookup, secondBlockMagicLookup = allShiftedValues(bzip2BlockMagic)
	})
}

// Scanner is a quick-and-dirty implementation of a scanner that
// returns runs of entire bz2 blocks. It works by splitting the input into
// blocks terminated by either the bz2 block magic or bz2 end of stream
// magic number sequences as documented in https://en.wikipedia.org/wiki/Bzip2
// with the cavaet that it does not detect non-byte aligned magic number
// sequences (bzip blocks are not byte aligned) and consequently it may
// return multiple blocks in a single scan. This is the 'quick-and-dirty'
// aspect! However, for large files it should be able to find sufficent
// numbers of such runs to benefit fro concurrency.
// The first block discovered will be the stream header and this
// is validated and consumed. The last block will be the stream trailer
// and this is also consumed and validated internally.
type Scanner struct {
	rd              io.Reader
	brd             *bufio.Reader
	err             error
	buf             []byte
	blockCRC        uint32
	bufBitSize      int
	bufBitOffset    int
	prevBitOffset   int
	first, done     bool
	header, trailer [4]byte
	blockSize       int
	maxPreamble     int
}

// NewScanner returns a new instance of Scanner.
func NewScanner(rd io.Reader, opts ...ScannerOption) *Scanner {
	Init()
	o := scannerOpts{
		// Allow enough overhead for the bzip block overhead of the coding tables
		// before the content stats.
		maxPreamble: 30 * 1024,
	}
	for _, fn := range opts {
		fn(&o)
	}
	bzs := &Scanner{
		rd:          rd,
		first:       true,
		maxPreamble: o.maxPreamble,
	}
	return bzs
}

func (sc *Scanner) scanHeader() bool {
	// Validate header.
	//	.magic:16              = 'BZ' signature/magic number
	//	.version:8             = 'h' for Bzip2 ('H'uffman coding),
	//                           '0' for //Bzip1 (deprecated)
	//	.hundred_k_blocksize:8 = '1'..'9' block-size 100 kB-900 kB
	//                           (uncompressed)
	n, err := sc.rd.Read(sc.header[:])
	if err != nil {
		sc.err = fmt.Errorf("failed to read stream header: %v\n", err)
	}
	if n != 4 {
		sc.err = fmt.Errorf("stream header is too small: %v", n)
		return false
	}
	if !bytes.Equal(sc.header[0:2], bzip2FileMagic) {
		sc.err = fmt.Errorf("wrong file magic: %x", sc.header[0:2])
		return false
	}
	if sc.header[2] != 'h' {
		sc.err = fmt.Errorf("wrong version: %c", sc.header[2])
		return false
	}
	if s := sc.header[3]; s < '0' || s > '9' {
		sc.err = fmt.Errorf("bad block size: %c", s)
		return false
	}
	sc.blockSize = 100 * 1000 * int(sc.header[3]-'0')
	sc.brd = bufio.NewReaderSize(sc.rd, sc.blockSize+sc.maxPreamble)
	return true
}

func readCRC(block []byte, shift int) uint32 {
	if len(block) < 4 {
		return 0
	}
	tmp := make([]byte, 5)
	copy(tmp, block[:5])
	for i := 8; i > shift; i-- {
		tmp = bitstreamShift(tmp)
	}
	return binary.BigEndian.Uint32(tmp[1:5])
}

// Scan returns true if there is a block to be returned.
func (sc *Scanner) Scan(ctx context.Context) bool {
	if sc.err != nil || sc.done {
		return false
	}
	select {
	case <-ctx.Done():
		sc.err = ctx.Err()
		return false
	default:
	}
	if sc.first {
		if !sc.scanHeader() {
			return false
		}
	}
	defer func() {
		sc.first = false
	}()

	eof := false
	lookahead := sc.blockSize + sc.maxPreamble
	buf, err := sc.brd.Peek(lookahead)
	if err != nil {
		if err != io.EOF {
			sc.err = err
			return false
		}
		eof = true
	}

	if sc.first {
		// Note: the block magic indicates the start of a block, not the
		// end of one. Therefore the first block must be handled specially.
		// If this is the first block, and it starts with a block magic
		// number, discard that block magic and search for the next one.
		if bytes.HasPrefix(buf, bzip2BlockMagic[:]) {
			sc.brd.Discard(len(bzip2BlockMagic))
			buf = buf[len(bzip2BlockMagic):]
			sc.bufBitOffset = 0
			sc.prevBitOffset = 0
		}
		if bytes.HasPrefix(buf, bzip2EOSMagic[:]) {
			// Handle the 'empty file/stream' case since for that
			// there os only an EOS block.
			return false
		}
	}

	// Look for the next block magic or eof.
	byteOffset, bitOffset := scanBitStream(firstBlockMagicLookup, secondBlockMagicLookup, buf)
	if byteOffset == -1 {
		if !eof {
			sc.err = fmt.Errorf("failed to find next block within expected max buffer size of %v", lookahead)
			return false
		}
		trailer, trailerSize, trailerOffset := findTrailingMagicAndCRC(buf, bzip2EOSMagic[:])
		if trailerSize == -1 {
			sc.err = fmt.Errorf("failed to find trailer")
			return false
		}
		copy(sc.trailer[:], trailer)
		sc.done = true
		sc.buf = make([]byte, len(buf)-trailerSize)
		copy(sc.buf, buf[:len(buf)-trailerSize])
		sc.bufBitOffset = sc.prevBitOffset
		sc.bufBitSize = (len(sc.buf) * 8)
		if trailerOffset > 0 {
			sc.bufBitSize += -8 + trailerOffset
		}
		sc.blockCRC = readCRC(sc.buf, sc.bufBitOffset)
		if sc.prevBitOffset > 0 {
			sc.bufBitSize -= sc.prevBitOffset
		}
		return true
	}
	sc.buf = make([]byte, byteOffset+1)
	copy(sc.buf, buf[:byteOffset+1])
	sc.bufBitOffset = sc.prevBitOffset
	sc.bufBitSize = (byteOffset * 8) + bitOffset
	sc.blockCRC = readCRC(sc.buf, sc.bufBitOffset)
	if sc.prevBitOffset > 0 {
		sc.bufBitSize -= sc.prevBitOffset
	}
	sc.prevBitOffset = bitOffset
	// skip the magic # before starting the search for the next magic #.
	sc.brd.Discard(byteOffset + len(bzip2BlockMagic))
	return true
}

// Header returns the stream header. It can only be called after Scan has been
// called at least once successfully.
func (sc *Scanner) Header() []byte {
	if sc.first {
		return nil
	}
	return sc.header[:]
}

// BlockSize returns the block size being used bu this stream.
// It can onlybe called after Scan has been called at least once successfully.
func (sc *Scanner) BlockSize() int {
	if sc.first {
		return 0
	}
	return sc.blockSize
}

// StreamCRC returns the stream CRC. It can only
// be called after Scan has returned false and sc.Err returns no error.
func (sc *Scanner) StreamCRC() uint32 {
	return binary.BigEndian.Uint32(sc.trailer[:])
}

// Blocks returns the current block and the bitoffset into that block
// at which the data starts as well as the crc
func (sc *Scanner) Block() (buf []byte, bitOffsize, sizeInBits int, crc uint32) {
	return sc.buf, sc.bufBitOffset, sc.bufBitSize, sc.blockCRC
}

// Err returns any error encountered by the scanner.
func (sc *Scanner) Err() error {
	return sc.err
}
