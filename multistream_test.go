package pbzip2_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/cosnicolaou/pbzip2"
)

func TestMultipleStreams(t *testing.T) {
	ctx := context.Background()
	var compressed, actual []byte
	for _, name := range []string{
		"hello", "empty", "300KB2", "300KB5", "hello", "empty",
	} {
		buf, _ := readFile(t, name)
		compressed = append(compressed, buf...)
		actual = append(actual, bzip2Data[name]...)
	}

	blockSizes := bci(253, 1610269, 864548, 2471788, 253)
	blockCRCs := bc(1324148790, 1186819639, 410614246, 1100438121, 1324148790)
	streamCRCs := bc(1324148790, 2500044168, 1100438121, 1324148790)
	streamBlockSizes := bci(9, 2, 5, 9)

	sc := pbzip2.NewScanner(bytes.NewBuffer(compressed))
	var nstream, nblock int
	for sc.Scan(ctx) {
		_, _, sizeBits, blockCRC, eos := sc.BlockEOS()
		if eos {
			if got, want := sc.StreamCRC(), streamCRCs[nstream]; got != want {
				t.Errorf("stream %v: got %v, want %v", nstream, got, want)
				return
			}
			if got, want := 100*1000*sc.BlockSize(), streamBlockSizes[nstream]; got != want {
				t.Errorf("stream %v: got %v, want %v", nstream, got, want)
			}
			nstream++
		}
		if got, want := sizeBits, blockSizes[nblock]; got != want {
			t.Errorf("block %v: got %v, want %v", nblock, got, want)
		}
		if got, want := blockCRC, blockCRCs[nblock]; got != want {
			t.Errorf("block %v: got %v, want %v", nblock, got, want)
		}
		nblock++
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if got, want := nstream, len(streamCRCs); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := nblock, len(blockCRCs); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	out := bytes.NewBuffer(nil)
	rd := pbzip2.NewReader(ctx, bytes.NewBuffer(compressed))
	_, err := io.Copy(out, rd)
	if err != nil {
		t.Error(err)
	}

	if got, want := out.Len(), len(actual); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := out.Bytes(), actual; !bytes.Equal(got, want) {
		t.Errorf("got %v, want %v", got[:10], want[:10])
	}
}
