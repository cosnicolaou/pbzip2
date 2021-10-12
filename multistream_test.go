package pbzip2_test

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"testing"

	"github.com/cosnicolaou/pbzip2"
	"github.com/cosnicolaou/pbzip2/internal/bzip2"
)

func concatFiles(t *testing.T, names ...string) (compressed, actual []byte) {
	for _, name := range names {
		buf, _ := readFile(t, name)
		compressed = append(compressed, buf...)
		actual = append(actual, bzip2Data[name]...)
	}
	return
}

func TestMultipleStreamsScan(t *testing.T) {
	ctx := context.Background()
	compressed, _ := concatFiles(t,
		"hello", "hello", "empty", "300KB2", "300KB5", "hello", "empty")

	// Values are taken from TestScan. Note that empty streams
	// are silently ignored.
	streamCRCs := bc(1324148790, 1324148790, 2500044168, 1100438121, 1324148790)
	blockCRCs := bc(1324148790, 1324148790, 1186819639, 410614246, 1100438121, 1324148790)
	blockSizes := bci(253, 253, 1610269, 864548, 2471788, 253)

	streamBlockSizes := bci(9, 9, 2, 5, 9)

	sc := pbzip2.NewScanner(bytes.NewBuffer(compressed))
	var nstream, nblock int
	for sc.Scan(ctx) {
		block := sc.Block()
		if block.EOS {
			if got, want := block.StreamCRC, streamCRCs[nstream]; got != want {
				t.Errorf("stream %v: stream CRC got %v, want %v", nstream, got, want)
			}
			if got, want := block.StreamBlockSize, 1000*100*streamBlockSizes[nstream]; got != want {
				t.Errorf("stream %v: stream block size got %v, want %v", nstream, got, want)
			}
			nstream++
		}
		if got, want := block.SizeInBits, blockSizes[nblock]; got != want {
			t.Errorf("block %v: block size got %v, want %v", nblock, got, want)
		}
		if got, want := block.CRC, blockCRCs[nblock]; got != want {
			t.Errorf("block %v: block CRC got 0x%08x, want 0x%08x", nblock, got, want)
		}
		rd := bzip2.NewBlockReader(block.StreamBlockSize, block.Data, block.BitOffset)
		if _, err := ioutil.ReadAll(rd); err != nil {
			t.Fatalf("block %v: EOS failed to decompress: %v\n", nblock, err)
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

}

func TestMultipleStreamsRead(t *testing.T) {
	ctx := context.Background()

	for i, tc := range [][]string{
		{"empty"},
		{"hello", "empty"},
		{"empty", "hello"},
		{"empty", "empty", "hello"},
		{"hello", "empty", "empty", "hello"},
		{"hello", "hello"},
		{"hello", "hello", "empty", "300KB2", "300KB5", "hello", "empty"},
	} {
		compressed, uncompressed := concatFiles(t, tc...)
		out := bytes.NewBuffer(nil)
		rd := pbzip2.NewReader(ctx, bytes.NewBuffer(compressed))
		n, err := io.Copy(out, rd)
		if err != nil {
			t.Errorf("%v: copy: %v", i, err)
			continue
		}
		if got, want := int(n), len(uncompressed); got != want {
			t.Errorf("%v: got %v, want %v", i, got, want)
		}
		if got, want := out.Len(), len(uncompressed); got != want {
			t.Errorf("%v: got %v, want %v", i, got, want)
		}
		if got, want := out.Bytes(), uncompressed; !bytes.Equal(got, want) {
			t.Errorf("%v: got %v, want %v", i, got[:10], want[:10])
		}
	}
}

func TestMultipleStreamErrors(t *testing.T) {
	ctx := context.Background()

	corruptedEmpty, _ := concatFiles(t, "hello", "empty", "empty")
	corruptedEmpty[len(corruptedEmpty)-2] = 0xff

	truncatedEmpty, _ := concatFiles(t, "hello", "empty", "empty")
	truncatedEmpty = truncatedEmpty[:len(truncatedEmpty)-2]

	trailingTruncatedEmpty, _ := concatFiles(t, "hello", "empty", "empty")
	trailingTruncatedEmpty = trailingTruncatedEmpty[:len(trailingTruncatedEmpty)-2]

	corruptedBlock, _ := concatFiles(t, "hello", "hello", "empty")
	corruptedBlock[len(corruptedBlock)-26] = 0xff
	for _, tc := range []struct {
		compressed []byte
		err        string
	}{
		{corruptedEmpty, "mismatched stream CRCs: calculated=0x4eece836 != stored=0x0000ff00"},
		{truncatedEmpty, "failed to find trailer"},
		{trailingTruncatedEmpty, "failed to find trailer"},
		{corruptedBlock, "block checksum mismatch"},
	} {
		rd := pbzip2.NewReader(ctx, bytes.NewBuffer(tc.compressed))
		out := &bytes.Buffer{}
		_, err := io.Copy(out, rd)
		if err == nil || err.Error() != tc.err {
			t.Errorf("missing or unexpected error: %v", err)
		}
	}
}
