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
	"strings"

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
	pretestBlockMagicLookup                       [256]bool
	firstBlockMagicLookup, secondBlockMagicLookup map[uint32]uint8
	blockMagic                                    [6]byte
	eosMagic                                      [6]byte
)

func init() {
	pretestBlockMagicLookup, firstBlockMagicLookup, secondBlockMagicLookup = bitstream.Init(bzip2.BlockMagic)
	copy(blockMagic[:], bzip2.BlockMagic[:])
	copy(eosMagic[:], bzip2.EOSMagic[:])
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
	rd                     io.Reader
	brd                    *bufio.Reader
	eos                    bool
	err                    error
	block                  CompressedBlock
	prevBitOffset          int
	first, done            bool
	maxPreamble            int
	currentStreamBlockSize int
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

func parseHeader(buf []byte) (int, error) {
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
	var header [4]byte
	n, err := sc.rd.Read(header[:])
	if err != nil {
		sc.err = fmt.Errorf("failed to read stream header: %v", err)
		return false
	}
	if n != 4 {
		sc.err = fmt.Errorf("stream header is too small: %v", n)
		return false
	}
	sc.currentStreamBlockSize, sc.err = parseHeader(header[:])
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

func prettyPrintBlock(block []byte) {
	for i := 0; i < len(block); i++ {
		if i > 0 && (i%32 == 0) {
			fmt.Println()
		}
		fmt.Printf("%02x ", block[i])
	}
	fmt.Println()
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
	lookahead := 9*100*1000 + sc.maxPreamble
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
			sc.block.BitOffset = 0
			sc.prevBitOffset = 0
		}
	}

	// Look for the next block magic or eof.
	byteOffset, bitOffset := bitstream.Scan(pretestBlockMagicLookup, firstBlockMagicLookup, secondBlockMagicLookup, buf)
	if byteOffset == -1 {
		if !eof {
			sc.err = fmt.Errorf("failed to find next block within expected max buffer size of %v", lookahead)
			return false
		}
		buf, _ := trimTrailingEmptyFiles(buf)
		// Note that if the stream is somehow corrupted and we don't find any
		// empty files here then the stream checksum check will fail or the
		// trailer won't be correctly located.
		return sc.handleEOF(buf)
	}

	if bitOffset == 0 {
		// Check for having skipped past an EOS block.
		if newStreamBlockSize, prevStreamCRC, consumed, trailerOffset, ok := handleSkippedEOS(buf[:byteOffset], byteOffset); ok {
			szBits := ((byteOffset - consumed) * 8) + trailerOffset - sc.prevBitOffset
			szBytes := szBits / 8
			if szBits%8 != 0 {
				szBytes++
			}
			if sc.prevBitOffset > 0 {
				szBytes++
			}
			// Note that size in bites needs to be the size of the previous
			// compressed block up to the EOS trailer and hence needs to take
			// the trailer offset into account.
			sc.initBlockValues(true, buf, szBytes, szBits, prevStreamCRC)
			sc.currentStreamBlockSize = newStreamBlockSize
			sc.prevBitOffset = bitOffset

			// skip the magic # before starting the search for the next magic #.
			sc.brd.Discard(byteOffset + len(blockMagic))
			return true
		}
	}
	sz := byteOffset
	if bitOffset > 0 {
		sz++
	}
	sc.initBlockValues(false, buf, sz, (byteOffset*8)+bitOffset-sc.prevBitOffset, 0)
	sc.prevBitOffset = bitOffset
	// skip the magic # before starting the search for the next magic #.
	sc.brd.Discard(byteOffset + len(blockMagic))
	return true
}

func (sc *Scanner) initBlockValues(eos bool, buf []byte, sz, szInBits int, streamCRC uint32) {
	sc.block = CompressedBlock{}
	sc.block.EOS = eos
	if sz > 0 {
		sc.block.Data = make([]byte, sz)
		copy(sc.block.Data, buf[:sz])
		sc.block.CRC = readCRC(buf, sc.prevBitOffset)
	}
	sc.block.BitOffset = sc.prevBitOffset
	sc.block.SizeInBits = szInBits
	sc.block.StreamBlockSize = sc.currentStreamBlockSize
	sc.block.StreamCRC = streamCRC
}

// trimTrailingEmptyFiles removes a trailing run of 1 or more empty files; an empty
// file has the following format:
// .magic:16
// .version:8
// .hundred_k_blocksize:8
// .eos_magic:48
// .crc:32
// .padding:0..7
//
// where the crc is all zeros and the hundred_k_block_size is 1..9.
func trimTrailingEmptyFiles(buf []byte) (trimmed []byte, n int) {
	for {
		var ok bool
		buf, ok = trimEmptyFile(buf)
		if !ok {
			return buf, n
		}
		n++
	}
}

func trimEmptyFile(buf []byte) ([]byte, bool) {
	trailer, trailerSize, trailerOffset := bitstream.FindTrailingMagicAndCRC(buf, eosMagic[:])
	if trailerSize != 10 || !bytes.Equal(trailer, []byte{0x0, 0x0, 0x0, 0x0}) {
		return buf, false
	}
	offset := 14 // 10 bytes of trailer, plus optional padding
	if trailerOffset > 0 {
		offset++
	}
	l := len(buf)
	if l < offset {
		return buf, false
	}
	if _, err := parseHeader(buf[l-offset:]); err != nil {
		return buf, false
	}
	return buf[:l-offset], true
}

// Check for having skipped past an EOS block.
//
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
// If an EOS block has been skipped then the compressed block must
// be preceeded by a valid file header, zero or empty files and
// then the EOS header. Recall that an empty file is a file
// header followed by an EOS block with a zero CRC.
//
// ...EOS[<empty-file>]*<hdr><blockMagic>
func handleSkippedEOS(buf []byte, byteOffset int) (newBlockSize int, prevCRC uint32, consumed, trailerOffset int, ok bool) {
	if byteOffset <= 4 {
		return
	}
	l := len(buf)
	newBlockSize, err := parseHeader(buf[l-4:])
	if err != nil {
		return
	}
	trimmed, n := trimTrailingEmptyFiles(buf[:l-4])

	trailer, trailerSize, trailerOffset := bitstream.FindTrailingMagicAndCRC(trimmed, eosMagic[:])
	if trailerSize != 10 {
		return
	}

	prevCRC = binary.BigEndian.Uint32(trailer)
	// size of header, trailer, plus any empty files.
	consumed = 4 + trailerSize + (n * 14)
	if trailerOffset > 0 {
		consumed++
	}
	ok = true
	return
}

func (sc *Scanner) handleEOF(buf []byte) bool {
	trailer, trailerSize, trailerOffset := bitstream.FindTrailingMagicAndCRC(buf, eosMagic[:])
	if trailerSize != 10 {
		sc.err = fmt.Errorf("failed to find trailer")
		return false
	}
	szBytes := len(buf) - trailerSize
	szBits := szBytes * 8
	if trailerOffset > 0 {
		szBits += -8 + trailerOffset
	}
	if sc.prevBitOffset > 0 {
		szBits -= sc.prevBitOffset
	}
	sc.initBlockValues(true, buf, szBytes, szBits, binary.BigEndian.Uint32(trailer))
	sc.done = true
	return true
}

// CompressedBlock represents a single bzip2 compressed block.
type CompressedBlock struct {
	// Buffer containing compressed data as a bitstream that starts at
	// BitOffset in the first byte of Buf and is SizeInBits large.
	Data            []byte
	BitOffset       int    // Compressed data starts at BitOffset in Data
	SizeInBits      int    // SizeInBits is the size of the compressed data in Data.
	CRC             uint32 // CRC for this block.
	StreamBlockSize int    // StreamBlockSize is the 1..9 *100*1000 compression block size specified when the stream was created.

	EOS       bool   // EOS has been detected.
	StreamCRC uint32 // CRC
}

func (b CompressedBlock) String() string {
	out := &strings.Builder{}
	level := b.StreamBlockSize / (100 * 1000)
	fmt.Fprintf(out, "@%v..%v bits: block CRC 0x%08x, bzip2 level %v", b.BitOffset, b.SizeInBits, b.CRC, -level)
	if b.EOS {
		fmt.Fprintf(out, " EOS: stream CRC 0x%08x", b.StreamCRC)
	}
	return out.String()
}

// Block returns the current block bzip2 compression block.
func (sc *Scanner) Block() CompressedBlock {
	return sc.block
}

// Err returns any error encountered by the scanner.
func (sc *Scanner) Err() error {
	return sc.err
}
