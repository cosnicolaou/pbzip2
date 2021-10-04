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
}

func ResetBlockMagic() {
	firstBlockMagicLookup, secondBlockMagicLookup = bitstream.Init()
	copy(blockMagic[:], bzip2.BlockMagic[:])
	copy(eosMagic[:], bzip2.EOSMagic[:])

}
