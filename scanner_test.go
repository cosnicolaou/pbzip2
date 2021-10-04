// Copyright 2020 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.
package pbzip2_test

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

	"github.com/cosnicolaou/pbzip2"
	"github.com/cosnicolaou/pbzip2/internal"
	"github.com/cosnicolaou/pbzip2/internal/bzip2"
)

var (
	bzip2Files map[string]string
	bzip2Data  map[string][]byte
)

// generateCompressedFiles writes a set of test compressed bzip2 test files.
func generateCompressedFiles(tmpdir string) (map[string]string, map[string][]byte, error) {
	names := map[string]string{}
	data := map[string][]byte{}
	for _, tc := range []struct {
		name      string
		data      []byte
		blockSize string
	}{
		{"empty", nil, "-1"},
		{"hello", []byte("hello world\n"), "-1"},
		{"100KB1", internal.GenPredictableRandomData(100 * 1024), "-1"},
		{"300KB1", internal.GenPredictableRandomData(300 * 1024), "-1"},
		{"400KB1", internal.GenPredictableRandomData(400 * 1024), "-1"},
		{"800KB1", internal.GenPredictableRandomData(800 * 1024), "-1"},
		{"900KB1", internal.GenPredictableRandomData(900 * 1024), "-1"},

		{"300KB3_Random", internal.GenReproducibleRandomData(300 * 1024), "-3"},
		{"900KB2_Random", internal.GenReproducibleRandomData(900 * 1024), "-2"},
		{"1033KB4_Random", internal.GenReproducibleRandomData(1033 * 1024), "-4"},
	} {
		filename := filepath.Join(tmpdir, tc.name)
		if err := internal.CreateBzipFile(filename, tc.blockSize, tc.data); err != nil {
			return nil, nil, err
		}
		names[tc.name] = filename
		data[tc.name] = tc.data

	}
	return names, data, nil
}

func TestMain(m *testing.M) {
	tmpdir, err := ioutil.TempDir("", "pbzip")
	if err != nil {
		panic(err)
	}
	bzip2Files, bzip2Data, err = generateCompressedFiles(tmpdir)
	if err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func readBzipFile(t *testing.T, filename string) []byte {
	gobuf, err := stdlibBzip2(filename + ".bz2")
	if err != nil {
		t.Fatalf("%v: %v", filename, err)
	}
	return gobuf
}

func readFile(t *testing.T, name string) ([]byte, int) {
	buf, err := os.ReadFile(bzip2Files[name] + ".bz2")
	if err != nil {
		t.Fatal(err)
	}
	return buf, len(buf) - 1
}

func openBzipFile(t *testing.T, filename string) io.ReadCloser {
	rd, err := os.Open(filename + ".bz2")
	if err != nil {
		t.Fatalf("%v: %v", filename, err)
	}
	return rd
}

func progress(n string, prgCh chan pbzip2.Progress) error {
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

func synchronousBlockBzip2(t *testing.T, sc *pbzip2.Scanner, name string, existing []byte) []byte {
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
	ctx := context.Background()
	// Note that gentestdata.go was used to generate the test cases below
	// using bzip2's stats feature.
	for _, tc := range []struct {
		name       string
		streamCRC  uint32
		blockCRCs  []uint32
		blockSizes []int
	}{
		{"empty", 0, bc(), bci()},
		{"hello", 1324148790, bc(1324148790), bci(253)},
		{"100KB1", 2846214228, bc(984137596, 3707025068), bci(806206, 22712)},
		{"300KB1", 2560071082,
			bc(984137596, 1527206082, 1102975844, 2729642890),
			bci(806206, 806273, 806182, 61754)},
		{"400KB1",
			182711008,
			bc(984137596, 1527206082, 1102975844, 1428961015, 3572671310),
			bci(806206, 806273, 806182, 806254, 81086)},
		{"800KB1",
			139967838,
			bc(984137596, 1527206082, 1102975844, 1428961015, 4117679320, 2969657708, 1647728401, 4168645754, 1334625769),
			bci(806206, 806273, 806182, 806254, 806158, 806323, 806263, 806295, 158358)},
		{"900KB1",
			1402104902,
			bc(984137596, 1527206082, 1102975844, 1428961015, 4117679320, 2969657708, 1647728401, 4168645754, 360300138, 4141343228),
			bci(806206, 806273, 806182, 806254, 806158, 806323, 806263, 806295, 806166, 177790)},
	} {
		filename := bzip2Files[tc.name]
		rd, stdlibData := openBzipFile(t, filename), readBzipFile(t, filename)

		defer rd.Close()

		var (
			sc     = pbzip2.NewScanner(rd)
			data   []byte
			n      int
			pwg    sync.WaitGroup
			pbuf   []byte
			perr   error
			prgCh  = make(chan pbzip2.Progress, 3)
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

		dc := pbzip2.NewDecompressor(ctx,
			pbzip2.BZConcurrency(3),
			pbzip2.BZSendUpdates(prgCh))

		pwg.Add(1)
		go func(n string) {
			pbuf, perr = ioutil.ReadAll(dc)
			pwg.Done()
		}(tc.name)

		for sc.Scan(ctx) {
			block, bitOffset, blockSizeBits, blockCRC := sc.Block()
			// Parallel decompress.
			dc.Decompress(sc.BlockSize(), block, bitOffset, blockSizeBits, blockCRC)
			if len(block) == 0 {
				continue
			}
			crcs = append(crcs, blockCRC)
			sizes = append(sizes, blockSizeBits)
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

		if got, want := data, bzip2Data[tc.name]; !bytes.Equal(got, want) {
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
		if got, want := pbuf, bzip2Data[tc.name]; !bytes.Equal(got, want) {
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
