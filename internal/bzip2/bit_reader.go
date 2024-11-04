// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bzip2

import (
	"bufio"
	"io"
)

// bitReader wraps an io.Reader and provides the ability to read values,
// bit-by-bit, from it. Its Read* methods don't return the usual error
// because the error handling was verbose. Instead, any error is kept and can
// be checked afterwards.
type bitReader struct {
	r         io.ByteReader
	n         uint64
	bits      uint
	err       error
	bytesRead uint
}

// newBitReader returns a new bitReader reading from r. If r is not
// already an io.ByteReader, it will be converted via a bufio.Reader.
func newBitReader(r io.Reader) bitReader {
	byter, ok := r.(io.ByteReader)
	if !ok {
		byter = bufio.NewReader(r)
	}
	return bitReader{r: byter}
}

// ReadBits64 reads the given number of bits and returns them in the
// least-significant part of a uint64. In the event of an error, it returns 0
// and the error can be obtained by calling Err().
func (br *bitReader) ReadBits64(bits uint) (n uint64) {
	for bits > br.bits {
		b, err := br.r.ReadByte()
		br.bytesRead++
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			br.err = err
			return 0
		}
		br.n <<= 8
		br.n |= uint64(b)
		br.bits += 8
	}

	// br.n looks like this (assuming that br.bits = 14 and bits = 6):
	// Bit: 111111
	//      5432109876543210
	//
	//         (6 bits, the desired output)
	//        |-----|
	//        V     V
	//      0101101101001110
	//        ^            ^
	//        |------------|
	//           br.bits (num valid bits)
	//
	// This the next line right shifts the desired bits into the
	// least-significant places and masks off anything above.
	n = (br.n >> (br.bits - bits)) & ((1 << bits) - 1)
	br.bits -= bits
	return
}

// PrefetchBytes reads `n` bytes from the underlying reader and stores them in the bitReader.
func (br *bitReader) PrefetchBytes(n uint) {
	if br.err != nil {
		return
	}
	for i := uint(0); i < n; i++ {
		b, err := br.r.ReadByte()
		br.bytesRead++
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			br.err = err
			return
		}
		br.n <<= 8
		br.n |= uint64(b)
		br.bits += 8
	}
}

func (br *bitReader) bitsUsed() uint {
	return (br.bytesRead * 8) - br.bits
}

// ReadBits reads the given number of bits and returns them as per ReadBits64, it
// must be called with bits <= 32.
func (br *bitReader) ReadBits(bits uint) (n int) {
	n64 := br.ReadBits64(bits)
	return int(n64) //#nosec G115 -- This is a false positive provided ReadBits is always called for < 32 bits.
}

func (br *bitReader) ReadBit() bool {
	n := br.ReadBits(1)
	return n != 0
}

func (br *bitReader) Err() error {
	return br.err
}
