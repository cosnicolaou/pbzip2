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
	pretestBlockMagicLookup, firstBlockMagicLookup, secondBlockMagicLookup = bitstream.Init(magic)
	copy(blockMagic[:], magic[:])
}

func ResetBlockMagic() {
	pretestBlockMagicLookup, firstBlockMagicLookup, secondBlockMagicLookup = bitstream.Init(bzip2.BlockMagic)
	copy(blockMagic[:], bzip2.BlockMagic[:])
	copy(eosMagic[:], bzip2.EOSMagic[:])
}
