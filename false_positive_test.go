package pbzip2_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/cosnicolaou/pbzip2"
	"github.com/cosnicolaou/pbzip2/internal/bitstream"
)

func TestHandlingFalsePositives(t *testing.T) {
	ctx := context.Background()
	filename := bzip2Files["300KB1"]
	rd := openBzipFile(t, filename)
	data, err := io.ReadAll(rd)
	if err != nil {
		t.Fatal(data)
	}

	//
	falsePositive := [6]byte{0xbb, 0x7a, 0x1b, 0xda, 0xf7, 0x27}
	//	d562    d965    c82c
	//	falsePositive = [6]byte{0x62, 0xd5, 0x65, 0xd9, 0x2c, 0xc8}

	pbzip2.SetCustomBlockMagic(falsePositive)
	defer pbzip2.ResetBlockMagic()

	// Block offsets in bits are from the output of gentestdata.go
	for _, offset := range []int{32, 806286, 1612607, 2418837} {
		bitstream.OverwriteAtBitOffset(data, offset, falsePositive[:])
	}

	fbl, sbl := bitstream.AllShiftedValues(falsePositive)
	o := 0
	prev := 0
	for {
		byteOffset, bitOffset := bitstream.Scan(fbl, sbl, data[o:])
		fmt.Printf("%d %d -> %d\n", byteOffset, bitOffset, prev+(byteOffset*8)+bitOffset)
		if byteOffset < 0 {
			break
		}
		o += byteOffset + 6
		prev += (byteOffset * 8) + bitOffset
	}

	brd := pbzip2.NewReader(ctx, bytes.NewBuffer(data))
	buf := bytes.NewBuffer(make([]byte, 0, 1000*1024))
	_, err = io.Copy(buf, brd)
	if err != nil {
		t.Error(err)
	}

	fmt.Printf("ERR: %v\n", err)

	fmt.Printf("%v: %v %v: %v\n", len(data), 806206, 22712, 806206+22712)

	//	t.FailNow()
}
