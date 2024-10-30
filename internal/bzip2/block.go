// Copyright 2019 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package bzip2

import (
	"bytes"
	"fmt"
	"io"
)

var (
	// FileMagic is the bzip2 file magic number.
	FileMagic = []byte{0x42, 0x5a} // "BZ"

	// BlockMagic is the magic number for each bzip data block.
	BlockMagic = [6]byte{0x31, 0x41, 0x59, 0x26, 0x53, 0x59}

	// EOSMagic is the magic number for each bzip end of stream block.
	EOSMagic = [6]byte{0x17, 0x72, 0x45, 0x38, 0x50, 0x90}
)

// BlockReader represents an io.Reader that can read a single bzip2 block.
type BlockReader struct {
	underlying *reader
	first      bool
	start      uint
	err        error
}

// NewBlockReader returns an io.Reader to read a single bzip2 block.
func NewBlockReader(blockSize int, src []byte, start int) io.Reader {
	if len(src) == 0 {
		return &BlockReader{err: io.EOF}
	}
	bz2 := new(reader)
	// mirror initialization from reader.setup()
	bz2.fileCRC = 0
	bz2.setupDone = true
	bz2.blockSize = blockSize
	bz2.tt = make([]uint32, bz2.blockSize)
	bz2.br = newBitReader(bytes.NewBuffer(src))
	return &BlockReader{underlying: bz2, first: true, start: uint(start)}
}

// Read implements io.Reader.
func (br *BlockReader) Read(buf []byte) (n int, err error) {
	if br.err != nil {
		return 0, br.err
	}
	if br.first {
		// skip to the start of the block.
		br.underlying.br.ReadBits(br.start)
		// We know we're at the start of a block.
		if err := br.underlying.readBlock(); err != nil {
			return 0, err
		}
		br.first = false
	}
	n = br.underlying.readFromBlock(buf)
	if n > 0 || len(buf) == 0 {
		br.underlying.blockCRC.update(buf[:n])
		return n, nil
	}
	if br.underlying.blockCRC.val != br.underlying.wantBlockCRC {
		return 0, fmt.Errorf("block checksum mismatch")
	}
	return n, io.EOF
}
