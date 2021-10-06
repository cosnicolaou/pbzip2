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

	"github.com/cosnicolaou/pbzip2/internal/bitstream"
	"github.com/cosnicolaou/pbzip2/internal/bzip2"
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
	firstBlockMagicLookup, secondBlockMagicLookup map[uint32]uint8
	blockMagic                                    [6]byte
	eosMagic                                      [6]byte
	zero                                          [256]bool
)

func init() {
	firstBlockMagicLookup, secondBlockMagicLookup = bitstream.Init()
	copy(blockMagic[:], bzip2.BlockMagic[:])
	copy(eosMagic[:], bzip2.EOSMagic[:])

	t2 := []byte{bzip2.BlockMagic[0], bzip2.BlockMagic[1], bzip2.BlockMagic[2]}
	for i := 0; i < 8; i++ {
		zero[t2[1]] = true
		bitstream.ShiftRight(t2)
	}
}

// Scanner returns runs of entire bz2 blocks. It works by splitting the input
// into blocks terminated by either the bz2 block magic or bz2 end of stream
// magic number sequences as documented in https://en.wikipedia.org/wiki/Bzip2.
// The scanner splits the magicc numbers into multiple lookup tables that
// include all possible shifted values to allow for efficient matching
// if bit (not byte) aligned values.
// The first block discovered will be the stream header and this
// is validated and consumed. The last block will be the stream trailer
// and this is also consumed and validated internally.
type Scanner struct {
	rd              io.Reader
	brd             *bufio.Reader
	eos             bool
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

func (sc *Scanner) parseHeader(buf []byte) (int, error) {
	// Validate header.
	//	.magic:16              = 'BZ' signature/magic number
	//	.version:8             = 'h' for Bzip2 ('H'uffman coding),
	//                           '0' for //Bzip1 (deprecated)
	//	.hundred_k_blocksize:8 = '1'..'9' block-size 100 kB-900 kB
	//                           (uncompressed)
	if !bytes.Equal(buf[0:2], bzip2.FileMagic) {
		return -1, fmt.Errorf("wrong file magic: %x", buf[0:2])
	}
	if buf[2] != 'h' {
		return -1, fmt.Errorf("wrong version: %c", buf[2])
	}
	if s := buf[3]; s < '0' || s > '9' {
		return -1, fmt.Errorf("bad block size: %c", s)

	}
	return 100 * 1000 * int(buf[3]-'0'), nil
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
		sc.err = fmt.Errorf("failed to read stream header: %v", err)
		return false
	}
	if n != 4 {
		sc.err = fmt.Errorf("stream header is too small: %v", n)
		return false
	}
	sc.blockSize, sc.err = sc.parseHeader(sc.header[:])
	if sc.err != nil {
		return false
	}
	// Allow for maximum possible block size.
	sc.brd = bufio.NewReaderSize(sc.rd, 9*100*1000+sc.maxPreamble)
	return true
}

func readCRC(block []byte, shift int) uint32 {
	if len(block) < 4 {
		return 0
	}
	tmp := make([]byte, 5)
	copy(tmp, block[:5])
	for i := 8; i > shift; i-- {
		tmp = bitstream.ShiftRight(tmp)
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

	sc.eos = false
	eof := false
	lookahead := sc.blockSize + sc.maxPreamble
	fmt.Printf(">>>>> %v -> %v\n", sc.blockSize, lookahead)
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
		if bytes.HasPrefix(buf, blockMagic[:]) {
			sc.brd.Discard(len(blockMagic))
			buf = buf[len(blockMagic):]
			sc.bufBitOffset = 0
			sc.prevBitOffset = 0
		} else if bytes.HasPrefix(buf, eosMagic[:]) {
			// Handle the 'empty file/stream' case since for that
			// there os only an EOS block.
			return false
		}
	}

	// Look for the next block magic or eof.
	byteOffset, bitOffset := bitstream.Scan(&zero, firstBlockMagicLookup, secondBlockMagicLookup, buf)
	if byteOffset == -1 {
		if !eof {
			sc.err = fmt.Errorf("failed to find next block within expected max buffer size of %v", lookahead)
			return false
		}
		return sc.handleEOF(buf)
	}

	sz := byteOffset
	if bitOffset > 0 {
		sz++
	}

	// Check for having skipped past an eos block.
	// The stream format is:
	// .magic:16
	// .version:8
	// .hundred_k_blocksize:8
	// .compressed_magic:48
	// .... data ....
	// .eos_magic:48
	// .crc:32
	// .padding:0..7
	//
	// So if a compressed_magic has been detected and it's the start of
	// a new stream, then the eos magic must be at most 8+8+16+[0..7]+crc
	// bits before it.
	//	if possibleEOSEoffset := byteOffset - (4 + 1 + 1); possibleEOSEoffset > 0 {
	if byteOffset > 4 {
		tbuf := buf[byteOffset-4:]
		blockSize, err := sc.parseHeader(tbuf)
		if err == nil {
			trailer, trailerSize, trailerOffset := bitstream.FindTrailingMagicAndCRC(tbuf, eosMagic[:])
			if trailerSize == 10 {
				fmt.Printf("T: %v %v %v\n", trailer, trailerSize, trailerOffset)
				copy(sc.header[:], tbuf[:4])
				copy(sc.trailer[:], trailer)
				fmt.Printf("TRAILER: %v .. %v: %02x\n", sc.trailer, eosMagic, tbuf[:12])
				sc.eos = true
				defer func() {
					sc.blockSize = blockSize
				}()
				_ = blockSize
			}
		}
		/*
			tbuf := buf[possibleEOSEoffset:]
			eosByteOffset, eosBitOffset := bitstream.Scan(firstEOSMagicLookup, secondEOSMagicLookup, tbuf)
			if eosByteOffset != -1 {
				fmt.Printf("EOS: %v %v\n", eosByteOffset, eosBitOffset)
				if blockSize, err := sc.parseHeader(buf[byteOffset-4:]); err != nil {
					fmt.Printf("oops... %v: %v\n", blockSize, err)
				} else {
					fmt.Printf("blocksize.,.. %v\n", blockSize)
					sc.eos = true
					// find the CRC...
					sc.blockCRC = readCRC(tbuf[eosBitOffset:], eosBitOffset+48)

					// avoid copying the preceeding header and eos block.
					fmt.Printf("size: %v\n", sz)
					//sz -= 4 //+ 10 - 1 // avoid copying the preceeding header and eos block.
				}

			}*/
	}

	sc.buf = make([]byte, sz)
	copy(sc.buf, buf[:sz])
	sc.bufBitOffset = sc.prevBitOffset
	sc.bufBitSize = (byteOffset * 8) + bitOffset

	sc.blockCRC = readCRC(sc.buf, sc.bufBitOffset)
	if sc.prevBitOffset > 0 {
		sc.bufBitSize -= sc.prevBitOffset
	}
	sc.prevBitOffset = bitOffset
	// skip the magic # before starting the search for the next magic #.
	sc.brd.Discard(byteOffset + len(blockMagic))

	return true
}

func (sc *Scanner) handleEOF(buf []byte) bool {
	trailer, trailerSize, trailerOffset := bitstream.FindTrailingMagicAndCRC(buf, eosMagic[:])
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

// Header returns the stream header. It can only be called after Scan has been
// called at least once successfully.
func (sc *Scanner) Header() []byte {
	if sc.first {
		return nil
	}
	return sc.header[:]
}

// BlockSize returns the block size being used by this stream.
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
func (sc *Scanner) Block() (buf []byte, bitOffset, sizeInBits int, crc uint32) {
	return sc.buf, sc.bufBitOffset, sc.bufBitSize, sc.blockCRC
}

// BlockEOS returns the current block and the bitoffset into that block
// at which the data starts as well as the crc
func (sc *Scanner) BlockEOS() (buf []byte, bitOffset, sizeInBits int, crc uint32, eos bool) {
	return sc.buf, sc.bufBitOffset, sc.bufBitSize, sc.blockCRC, sc.eos
}

// Err returns any error encountered by the scanner.
func (sc *Scanner) Err() error {
	return sc.err
}
