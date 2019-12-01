// +build ignore

package main

import (
	"encoding/binary"
	"fmt"
	"time"
)

func allPatterns(magic [6]byte) map[uint64]struct{} {
	magic64 := make([]uint64, 256*256)
	magicMap := make(map[uint64]struct{}, 256*256*128)
	o := 0
	val := [8]byte{}
	vs := val[:]
	copy(val[:], magic[:])
	for i := 0; i < 256; i++ {
		val[6] = uint8(i)
		for j := 0; j < 256; j++ {
			val[7] = uint8(j)
			v64 := binary.LittleEndian.Uint64(vs)
			magic64[o] = v64
			o++
			magicMap[v64] = struct{}{}
		}
	}
	for _, m64 := range magic64 {
		for s := 1; s <= 7; s++ {
			v64 := m64 >> 1
			magicMap[v64] = struct{}{}
			magicMap[(1<<63)|v64] = struct{}{}
		}
	}
	return nil
}

var bzip2BlockMagic = [6]byte{0x31, 0x41, 0x59, 0x26, 0x53, 0x59}

func main() {
	start := time.Now()
	allPatterns(bzip2BlockMagic)
	fmt.Printf("took: %v\n", time.Since(start))
}
