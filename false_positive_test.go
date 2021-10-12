// Copyright 2021 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package pbzip2_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"testing"

	"github.com/cosnicolaou/pbzip2"
	"github.com/cosnicolaou/pbzip2/internal/bitstream"
)

func TestHandlingFalsePositives(t *testing.T) {
	ctx := context.Background()
	filename := bzip2Files["300KB1"]

	rd := openBzipFile(t, filename)
	origData, err := ioutil.ReadAll(rd)
	if err != nil {
		t.Fatal(err)
	}
	godata := readBzipFile(t, filename)

	defer pbzip2.ResetBlockMagic()

	// Fake a false positive by finding some sequences that occur as
	// data and then changing the block magic values to be these
	// naturally ocurring sequences.
	for i, falsePositiveRange := range [][8]byte{
		{0xae, 0x91, 0xff, 0x6b, 0x72, 0xb1, 0xa4, 0x7a},
		{0xed, 0xbb, 0x7a, 0x1b, 0xda, 0xf7, 0x27, 0x57},
	} {

		// Test with shifted values of the magic numbers above.
		for s := 0; s < 8; s++ {
			data := make([]byte, len(origData))
			copy(data, origData)

			var (
				falsePositive [6]byte
				tmp           [8]byte
			)
			copy(tmp[:], falsePositiveRange[:])
			for i := 0; i < s; i++ {
				bitstream.ShiftRight(tmp[:])
			}
			copy(falsePositive[:], tmp[1:7])

			fmt.Printf("magic: %08b\n", falsePositive)

			// Block offsets in bits are from the output of gentestdata.go
			for _, offset := range []int{32, 806286, 1612607, 2418837} {
				bitstream.OverwriteAtBitOffset(data, offset, falsePositive[:])
			}

			pbzip2.SetCustomBlockMagic(falsePositive)
			brd := pbzip2.NewReader(ctx, bytes.NewBuffer(data))
			buf := bytes.NewBuffer(make([]byte, 0, 1000*1024))
			_, err = io.Copy(buf, brd)
			if err != nil {
				t.Error(err)
			}

			if got, want := buf.Bytes(), godata; !bytes.Equal(got, want) {
				if testing.Verbose() {
					fmt.Printf("got\n")
					prettyPrintBlock(got)
					fmt.Printf("want\n")
					prettyPrintBlock(want)
				}
				t.Errorf("%v: got %v, want %v", i, len(got), len(want))
			}
		}
	}
}

func prettyPrintBlock(block []byte) {
	for i := 0; i < len(block); i++ {
		if i > 0 && (i%32 == 0) {
			fmt.Println()
		}
		fmt.Printf("%02x ", block[i])
	}
	fmt.Println()
}
