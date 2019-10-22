// Copyright 2019 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.
package pbzip2

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
)

type scannerOpts struct {
	buf         []byte
	max         int
	initialSize int
}

// ScannerOption represenst an option to NewBZ2BlockScanner.
type ScannerOption func(*scannerOpts)

// ScannerBufferOption specifies the buffer and max size (see bufio.Scanner.Buffer)
// to use with the underlying scanner.
func ScannerBufferOption(buf []byte, max int) ScannerOption {
	return func(o *scannerOpts) {
		o.buf = buf
		o.max = max
	}
}

// ScannerInitialSampleSize sets the initial size of the slice used
// to record the size of each scanned line. Set it to any non-zero value
// to enable recording of the sizes of the inputs.
func ScannerInitialSampleSize(max int) ScannerOption {
	return func(o *scannerOpts) {
		o.initialSize = max
	}
}

// See https://en.wikipedia.org/wiki/Bzip2 for an explanation of the file
// format.
var bzip2FileMagic = []byte{0x42, 0x5a} // "BZ"

var bzip2BlockMagic = []byte{0x31, 0x41, 0x59, 0x26, 0x53, 0x59}
var bzip2EOSMagic = []byte{0x17, 0x72, 0x45, 0x38, 0x50, 0x90}

func (sc *Scanner) blockSplit(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.Index(data, bzip2BlockMagic); i >= 0 {
		fmt.Printf("FOUND BLOCK AT.. %v\n", i)
		return i + len(bzip2BlockMagic), data[:i], nil
	}

	// if at EOF ... look for trailer by working backwards.

	if i := bytes.Index(data, bzip2EOSMagic); i >= 0 {
		fmt.Printf("FOUND EOS AT.. %v\n", i)
		return i + len(bzip2EOSMagic), data[:i], nil
	}
	return 0, nil, nil
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
	sc        *bufio.Scanner
	err       error
	first     bool
	header    [4]byte
	blockSize int
	max       int
	sizes     []float64
}

// NewScanner returns a new instance of Scanner.
func NewScanner(rd io.Reader, opts ...ScannerOption) *Scanner {
	scopts := scannerOpts{
		buf: make([]byte, 0, 10*1024*1024),
		max: 10 * 1024 * 1024,
	}
	for _, fn := range opts {
		fn(&scopts)
	}
	underlying := bufio.NewScanner(rd)
	underlying.Buffer(scopts.buf, scopts.max)
	bzs := &Scanner{
		sc:    underlying,
		first: true,
	}
	if s := scopts.initialSize; s > 0 {
		bzs.sizes = make([]float64, 0, s)
	}
	underlying.Split(bzs.blockSplit)
	return bzs
}

func (sc *Scanner) scanHeader() bool {
	if !sc.first {
		return true
	}
	if !sc.sc.Scan() {
		if err := sc.sc.Err(); err != nil {
			sc.err = err
			return false
		}
		sc.err = fmt.Errorf("failed to find stream header")
		return false
	}
	sc.first = false
	// Validate header.
	//	.magic:16              = 'BZ' signature/magic number
	//	.version:8             = 'h' for Bzip2 ('H'uffman coding),
	//                           '0' for //Bzip1 (deprecated)
	//	.hundred_k_blocksize:8 = '1'..'9' block-size 100 kB-900 kB
	//                           (uncompressed)
	header := sc.sc.Bytes()
	if len(header) != 4 {
		sc.err = fmt.Errorf("stream header is the wrong size: %v", len(header))
		return false
	}
	if !bytes.Equal(header[0:2], bzip2FileMagic) {
		sc.err = fmt.Errorf("wrong file magic: %x", header[0:2])
		return false
	}
	if header[2] != 'h' {
		sc.err = fmt.Errorf("wrong version: %c", header[2])
		return false
	}
	if s := header[3]; s < '0' || s > '9' {
		sc.err = fmt.Errorf("bad block size: %c", s)
		return false
	}
	sc.blockSize = 100 * 1000 * int(header[3]-'0')
	copy(sc.header[:], header)
	return true
}

// Scan returns true if there a block to be returned.
func (sc *Scanner) Scan() bool {
	if sc.err != nil {
		return false
	}
	if !sc.scanHeader() {
		return false
	}
	fmt.Printf("scanning... \n")
	if sc.sc.Scan() {
		fmt.Printf("scanning true \n")

		l := len(sc.sc.Bytes())
		if l > sc.max {
			sc.max = l
		}
		if sc.sizes != nil {
			sc.sizes = append(sc.sizes, float64(l))
		}

		return true
	}
	fmt.Printf("scanning false \n")

	return false
}

// Sizes returns a slice of the sizes of each input line. It is returned
// as a float64 to simplify using it with various stats packages. The max
// size is tracked also.
func (sc *Scanner) Sizes() ([]float64, int) {
	return nil, sc.max
}

// StreamHeader returns the stream header. It can only
// be called after Scan has been called at least once successfully.
func (sc *Scanner) StreamHeader() []byte {
	if sc.first {
		return nil
	}
	return sc.header[:]
}

// BlockSize returns the stream's block size in bytes. It can only
// be called after Scan has been called at least once successfully.
func (sc *Scanner) BlockSize() int {
	if sc.first {
		return 0
	}
	return sc.blockSize
}

// Blocks returns the currently scanned run of blocks. It is returned
// as a copy of the underlying data to allow for concurrent use
// and does not include the magic number.
func (sc *Scanner) Blocks() []byte {
	cpy := make([]byte, sc.underlying.Bytes())
	copy(cpy, sc.underlying.Bytes())
	return cpy
}

// Err returns any error encountered by the scanner.
func (sc *Scanner) Err() error {
	if sc.err != nil {
		return sc.err
	}
	return sc.sc.Err()
}
