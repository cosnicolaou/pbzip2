package pbzip2

import (
	"fmt"
	"io"
	"testing"
)

func TestHandlingFalsePositives(t *testing.T) {
	filename := bzip2Files["300KB1"]
	rd := openBzipFile(t, filename)
	data, err := io.ReadAll(rd)
	if err != nil {
		t.Fatal(data)
	}

	// Read from the outout of gentestdata.go
	blockOffsets := []uint{32, 806286, 1612607, 2418837}

	corrupted := data[:9000]
	corrupted = append(corrupted)

	fmt.Printf("%v: %v %v: %v\n", len(data), 806206, 22712, 806206+22712)

	t.FailNow()
}
