// Copyright 2021 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"time"
)

// Seed for the pseudorandom generator, must be shared with gentestdata.go
const fixdRandSeed = 0x1234

var randSource rand.Source

func init() {
	randSeed := time.Now().UnixNano()
	fmt.Printf("rand seed for GenReproducibleRandomData: %v\n", randSeed)
	randSource = rand.NewSource(randSeed)
}

// GenPredictableRandomData generates random data starting with a fixed
// known seed.
func GenPredictableRandomData(size int) []byte {
	gen := rand.New(rand.NewSource(fixdRandSeed))
	out := make([]byte, size)
	for i := range out {
		out[i] = byte(gen.Intn(256))
	}
	return out
}

// GenReproducibleRandomData uses the random # seed printed out by this
// file's init function.
func GenReproducibleRandomData(size int) []byte {
	gen := rand.New(randSource)
	out := make([]byte, size)
	for i := range out {
		out[i] = byte(gen.Intn(256))
	}
	return out
}

// CreateBzipFile creates a bzip file of the supplied raw data.
func CreateBzipFile(filename, blockSize string, data []byte) error {
	if err := os.WriteFile(filename, data, 0660); err != nil {
		return fmt.Errorf("write file: %v: %v", filename, err)
	}
	cmd := exec.Command("bzip2", filename, blockSize)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to run bzip2 on %v: %v: %v", filename, err, string(output))
	}
	return nil
}

// FirstN returns at most the first n bytes of b.
func FirstN(n int, b []byte) []byte {
	if len(b) > n {
		return b[:n]
	}
	return b
}
