// Copyright 2021 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package pbzip2

import (
	"sync/atomic"

	"github.com/cosnicolaou/pbzip2/internal/bitstream"
	"github.com/cosnicolaou/pbzip2/internal/bzip2"
)

func GetNumDecompressionGoRoutines() int64 {
	return atomic.LoadInt64(&numDecompressionGoRoutines)
}

func SetCustomBlockMagic(magic [6]byte) {
	firstBlockMagicLookup, secondBlockMagicLookup =
		bitstream.AllShiftedValues(magic)
	copy(blockMagic[:], magic[:])
	t2 := []byte{magic[0], magic[1], magic[2]}
	for i := 0; i < 8; i++ {
		zero[t2[1]] = true
		bitstream.ShiftRight(t2)
	}
}

func ResetBlockMagic() {
	firstBlockMagicLookup, secondBlockMagicLookup = bitstream.Init()
	copy(blockMagic[:], bzip2.BlockMagic[:])
	copy(eosMagic[:], bzip2.EOSMagic[:])
	t2 := []byte{bzip2.BlockMagic[0], bzip2.BlockMagic[1], bzip2.BlockMagic[2]}
	for i := 0; i < 8; i++ {
		zero[t2[1]] = true
		bitstream.ShiftRight(t2)
	}
}
