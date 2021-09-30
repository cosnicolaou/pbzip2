// Copyright 2020 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.
package pbzip2

import (
	"bytes"
	gobzip2 "compress/bzip2"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/cosnicolaou/pbzip2/bzip2"
	"github.com/cosnicolaou/pbzip2/internal"
)

func createBzipFile(t *testing.T, filename, blockSize string, data []byte) (io.ReadCloser, []byte) {
	if err := internal.CreateBzipFile(filename, blockSize, data); err != nil {
		return nil, nil
	}
	gobuf, err := stdlibBzip2(filename + ".bz2")
	if err != nil {
		t.Fatalf("%v: %v", filename, err)
	}
	rd, err := os.Open(filename + ".bz2")
	if err != nil {
		t.Fatalf("%v: %v", filename, err)
	}
	return rd, gobuf
}

func progress(n string, prgCh chan Progress) error {
	next := uint64(1)
	for p := range prgCh {
		fmt.Printf("%#v\n", p)
		if p.Block != next {
			return fmt.Errorf("%v: out of sequence block %#v", n, p)
		}
		next++
	}
	return nil
}

func stdlibBzip2(filename string) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	buf, err := ioutil.ReadAll(gobzip2.NewReader(f))
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func synchronousBlockBzip2(t *testing.T, sc *Scanner, name string, existing []byte) []byte {
	block, bitOffset, _, _ := sc.Block()
	rd := bzip2.NewBlockReader(sc.BlockSize(), block, bitOffset)
	buf, err := ioutil.ReadAll(rd)
	if err != nil {
		t.Errorf("%v: decompression failed: %v", name, err)
		return nil
	}
	return append(existing, buf...)
}

func bc(c ...uint32) []uint32 {
	return c
}

func bci(c ...int) []int {
	return c
}

func TestScan(t *testing.T) {
	tmpdir := t.TempDir()
	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		data       []byte
		blockSize  string
		streamCRC  uint32
		blockCRCs  []uint32
		blockSizes []int
	}{
		{"empty", nil, "-1", 0, bc(), bci()},
		{"hello", []byte("hello world\n"), "-1",
			1324148790,
			bc(1324148790),
			bci(253)},
		{"100KB1", internal.GenPredictableRandomData(100 * 1024), "-1",
			2846214228,
			bc(984137596, 3707025068),
			bci(806206, 22712)},
		{"300KB1", internal.GenPredictableRandomData(300 * 1024), "-1",
			2560071082,
			bc(984137596, 1527206082, 1102975844, 2729642890),
			bci(806206, 806273, 806182, 61754)},
		{"400KB1", internal.GenPredictableRandomData(400 * 1024), "-1",
			182711008,
			bc(984137596, 1527206082, 1102975844, 1428961015, 3572671310),
			bci(806206, 806273, 806182, 806254, 81086)},
		{"800KB1", internal.GenPredictableRandomData(800 * 1024), "-1",
			139967838,
			bc(984137596, 1527206082, 1102975844, 1428961015, 4117679320, 2969657708, 1647728401, 4168645754, 1334625769),
			bci(806206, 806273, 806182, 806254, 806158, 806323, 806263, 806295, 158358)},
		{"900KB1", internal.GenPredictableRandomData(900 * 1024), "-1",
			1402104902,
			bc(984137596, 1527206082, 1102975844, 1428961015, 4117679320, 2969657708, 1647728401, 4168645754, 360300138, 4141343228),
			bci(806206, 806273, 806182, 806254, 806158, 806323, 806263, 806295, 806166, 177790)},
	} {
		rd, stdlibData := createBzipFile(t,
			filepath.Join(tmpdir, tc.name),
			tc.blockSize,
			tc.data)
		defer rd.Close()

		var (
			sc     = NewScanner(rd)
			data   []byte
			n      int
			pwg    sync.WaitGroup
			pbuf   []byte
			perr   error
			prgCh  = make(chan Progress, 3)
			prgwg  sync.WaitGroup
			prgerr error
			crcs   []uint32
			sizes  []int
		)

		prgwg.Add(1)
		go func(n string) {
			prgerr = progress(n, prgCh)
			prgwg.Done()
		}(tc.name)

		dc := NewDecompressor(ctx, BZConcurrency(3), BZSendUpdates(prgCh))

		pwg.Add(1)
		go func(n string) {
			pbuf, perr = ioutil.ReadAll(dc)
			pwg.Done()
		}(tc.name)

		for sc.Scan(ctx) {
			block, bitOffset, blockSize, blockCRC := sc.Block()
			// Parallel decompress.
			dc.Decompress(sc.BlockSize(), block, bitOffset, blockCRC)
			if len(block) == 0 {
				continue
			}
			crcs = append(crcs, blockCRC)
			sizes = append(sizes, blockSize)
			// Synchronous scan + decompress.
			data = synchronousBlockBzip2(t, sc, tc.name, data)
			n++
		}
		if err := sc.Err(); err != nil {
			t.Errorf("%v: scan failed: %v", tc.name, err)
			continue
		}

		if got, want := sc.StreamCRC(), tc.streamCRC; got != want {
			t.Errorf("%v: got %v, want %v", tc.name, got, want)
		}

		if got, want := crcs, tc.blockCRCs; !reflect.DeepEqual(got, want) {
			t.Errorf("%v: got %v, want %v", tc.name, got, want)
		}

		if got, want := sizes, tc.blockSizes; !reflect.DeepEqual(got, want) {
			t.Errorf("%v: got %v, want %v", tc.name, got, want)
		}

		if got, want := data, tc.data; !bytes.Equal(got, want) {
			t.Errorf("%v: got %v..., want %v...", tc.name, internal.FirstN(10, got), internal.FirstN(10, want))
		}

		if got, want := data, stdlibData; !bytes.Equal(got, want) {
			t.Errorf("%v: got %v..., want %v...", tc.name, internal.FirstN(10, got), internal.FirstN(10, want))
		}

		crc, err := dc.Finish()
		if err != nil {
			t.Errorf("Finish: %v", err)
		}
		if got, want := crc, tc.streamCRC; got != want {
			t.Errorf("%v: got %v, want %v", tc.name, got, want)
		}
		pwg.Wait()
		if err := perr; err != nil {
			t.Errorf("failed to read from parallel decompressor: %v", err)
		}
		if got, want := pbuf, tc.data; !bytes.Equal(got, want) {
			t.Errorf("%v: got %v..., want %v...", tc.name, internal.FirstN(10, got), internal.FirstN(10, want))
		}
		close(prgCh)
		prgwg.Wait()
		if err := prgerr; err != nil {
			t.Errorf("progress indicator error: %v", err)
		}
	}
}

func TestEmpty(t *testing.T) {
	br := bzip2.NewBlockReader(1024, nil, 0)
	buf := make([]byte, 1024)
	n, err := br.Read(buf)
	if got, want := err, io.EOF; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := n, 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
