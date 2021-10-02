package pbzip2

import "sync/atomic"

func GetNumDecompressionGoRoutines() int64 {
	return atomic.LoadInt64(&numDecompressionGoRoutines)
}
