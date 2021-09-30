package internal

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os/exec"
)

// Seed for the pseudorandom generator, must be shared with gentestdata.go
const randSeed = 0x1234

func GenPredictableRandomData(size int) []byte {
	gen := rand.New(rand.NewSource(randSeed))
	out := make([]byte, size)
	for i := range out {
		out[i] = byte(gen.Intn(256))
	}
	return out
}

func CreateBzipFile(filename, blockSize string, data []byte) error {
	if err := ioutil.WriteFile(filename, data, 0660); err != nil {
		return fmt.Errorf("write file: %v: %v", filename, err)
	}
	cmd := exec.Command("bzip2", filename, blockSize)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to run bzip2 on %v: %v: %v", filename, err, string(output))
	}
	return nil
}

func FirstN(n int, b []byte) []byte {
	if len(b) > n {
		return b[:n]
	}
	return b
}
