// +build ignore

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"

	"github.com/cosnicolaou/pbzip2/bzip2"
)

// Seed for the pseudorandom generator, must be shared with scanner_test.go
const randSeed = 0x1234

func genPredictableRandomData(size int) []byte {
	gen := rand.New(rand.NewSource(randSeed))
	out := make([]byte, size)
	for i := range out {
		out[i] = byte(gen.Intn(256))
	}
	return out
}

func main() {
	for _, tc := range []struct {
		name string
		data []byte
		args []string
	}{
		{"empty", nil, nil},
		{"hello", []byte("hello world\n"), nil},
		{"100KB1", genPredictableRandomData(100 * 1024), []string{"-1"}},
		{"300KB1", genPredictableRandomData(300 * 1024), []string{"-1"}},
		{"400KB1", genPredictableRandomData(400 * 1024), []string{"-1"}},
	} {
		raw, bz2 := tc.name, tc.name+".bz2"
		os.Remove(raw)
		os.Remove(bz2)
		if err := ioutil.WriteFile(raw, tc.data, 0660); err != nil {
			log.Fatalf("write file: %v: %v", raw, err)
		}
		cmd := exec.Command("bzip2", append([]string{raw}, tc.args...)...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Fatalf("failed to run bzip2: %v: %v", err, string(output))
		}
		compressed, err := os.Open(bz2)
		if err != nil {
			log.Fatalf("failed to open: %v: %v", bz2, err)
		}
		rd := bzip2.NewReaderWithStats(compressed)
		if _, err = ioutil.ReadAll(rd); err != nil {
			log.Fatalf("failed to read: %v: %v", bz2, err)
		}
		stats := bzip2.StreamStats(rd)
		fmt.Printf("=== %v ===\n", tc.name)
		fmt.Printf("Block CRCS           : %v\n", stats.BlockCRCs)
		fmt.Printf("Stream/File CRC      : %v\n", stats.StreamCRC)
		fmt.Printf("Block Offsets        : %v\n", stats.BlockStartOffsets)
		fmt.Printf("End of Stream Offset : %v\n", stats.EndOfStreamOffset)
		var sizes []uint
		if len(stats.BlockStartOffsets) > 0  {
			offsets := make([]uint, len(stats.BlockStartOffsets)+1)
			for i := 0; i< len(offsets)-1 ; i++ {
				offsets[i] = stats.BlockStartOffsets[i]
			}
			offsets[len(offsets)-1] = stats.EndOfStreamOffset
			sizes = make([]uint, len(stats.BlockStartOffsets))
			for i := 0; i < len(sizes); i++ {
				sizes[i] = offsets[i+1] - offsets[i] - 48 // subtract size of magic #
			}
		}
		fmt.Printf("Block Sizes          : %v\n",sizes)
	}
}

