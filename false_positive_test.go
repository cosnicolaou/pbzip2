package pbzip2_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/cosnicolaou/pbzip2"
)

func TestHandlingFalsePositives(t *testing.T) {
	ctx := context.Background()
	filename := bzip2Files["300KB1"]
	rd := openBzipFile(t, filename)
	data, err := io.ReadAll(rd)
	if err != nil {
		t.Fatal(data)
	}

	// Read from the outout of gentestdata.go
	blockOffsets := []uint{32, 806286, 1612607, 2418837}

	corrupted := data[:9000]
	corrupted = append(corrupted, pbzip2.BlockMagic()...)
	corrupted = append(corrupted, data[9000:]...)

	brd := pbzip2.NewReader(ctx, bytes.NewBuffer(corrupted))
	buf := bytes.NewBuffer(make([]byte, 0, 100*1024))
	_, err = io.Copy(buf, brd)

	fmt.Printf("ERR: %v\n", err)

	fmt.Printf("%v .. %v %v\n", len(data), len(corrupted), len(corrupted)-len(data))

	_ = blockOffsets

	fmt.Printf("%v: %v %v: %v\n", len(data), 806206, 22712, 806206+22712)

	//	t.FailNow()
}
