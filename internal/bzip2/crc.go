package bzip2

import (
	"hash/crc32"
	"math/bits"
)

type crc struct {
	val uint32
	buf [256]byte
}

func (c *crc) update(buf []byte) {
	cval := bits.Reverse32(c.val)
	for len(buf) > 0 {
		n := copy(c.buf[:], buf)
		buf = buf[n:]
		for i, b := range c.buf[:n] {
			c.buf[byte(i)] = bits.Reverse8(b)
		}
		cval = crc32.Update(cval, crc32.IEEETable, c.buf[:n])
	}
	c.val = bits.Reverse32(cval)
}
