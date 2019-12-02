package bzip2

import (
	"bytes"
	"fmt"
	"io"
)

type BlockReader struct {
	underlying *reader
	err        error
}

// NewBlockReader returns an io.Reader to read a single bzip2 block.
func NewBlockReader(blockSize int, src []byte, start int) io.Reader {
	bz2 := new(reader)
	// mirror initialization from reader.setup()
	bz2.fileCRC = 0
	bz2.setupDone = true
	bz2.blockSize = blockSize
	bz2.tt = make([]uint32, bz2.blockSize)
	bz2.br = newBitReader(bytes.NewBuffer(src))
	// skip to the start of the block.
	bz2.br.ReadBits(uint(start))
	// We know we're at the start of a block.
	if err := bz2.readBlock(); err != nil {
		return &BlockReader{err: err}
	}
	return &BlockReader{underlying: bz2}
}

// Read implements io.Reader.
func (br *BlockReader) Read(buf []byte) (n int, err error) {
	if br.err != nil {
		return 0, br.err
	}
	n = br.underlying.readFromBlock(buf)
	if n > 0 || len(buf) == 0 {
		br.underlying.blockCRC = updateCRC(br.underlying.blockCRC, buf[:n])
		return n, nil
	}
	if br.underlying.blockCRC != br.underlying.wantBlockCRC {
		return 0, fmt.Errorf("block checksum mismatch")
	}
	return n, io.EOF
}
