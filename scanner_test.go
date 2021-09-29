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
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cosnicolaou/pbzip2/bzip2"
)

func getData(name string) (reader io.ReadCloser, original []byte, err error) {
	reader, err = os.Open(filepath.Join("testdata", name+".txt.bz2"))
	if err != nil {
		return
	}
	original, err = ioutil.ReadFile(filepath.Join("testdata", name+".txt.bz2"))
	return
}

// Seed for the pseudorandom generator, must be shared with gentestdata.go
const randSeed = 0x1234

func genPredictableRandomData(size int) []byte {
	gen := rand.New(rand.NewSource(randSeed))
	out := make([]byte, size)
	for i := range out {
		out[i] = byte(gen.Intn(256))
	}
	return out
}

func createBzipFile(name, blockSize string, data []byte) (io.ReadCloser, error) {
	os.Remove(name)
	os.Remove(name + ".bz2")
	if err := ioutil.WriteFile(name, data, 0660); err != nil {
		return nil, fmt.Errorf("write file: %v: %v", name, err)
	}
	cmd := exec.Command("bzip2", name, blockSize)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to run bzip2: %v: %v", err, string(output))
	}
	return os.Open(filepath.Join(name + ".bz2"))
}

func TestScan(t *testing.T) {
	ctx := context.Background()
	//	Verbose = true
	bc := func(c ...uint32) []uint32 {
		return c
	}
	bci := func(c ...int) []int {
		return c
	}
	tmpdir, err := ioutil.TempDir("", "pbzip")
	if err != nil {
		t.Fatalf("failed to get tmp dir: %v", err)
	}
	defer os.RemoveAll(tmpdir)
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
		{"100KB1", genPredictableRandomData(100 * 1024), "-1",
			2846214228,
			bc(984137596, 3707025068),
			bci(806206, 22712)},
		{"300KB1", genPredictableRandomData(300 * 1024), "-1",
			2560071082,
			bc(984137596, 1527206082, 1102975844, 2729642890),
			bci(806206, 806273, 806182, 61754)},
		{"400KB1", genPredictableRandomData(400 * 1024), "-1",
			182711008,
			bc(984137596, 1527206082, 1102975844, 1428961015, 3572671310),
			bci(806206, 806273, 806182, 806254, 81086)},
		{"800KB1", genPredictableRandomData(800 * 1024), "-1",
			139967838,
			bc(984137596, 1527206082, 1102975844, 1428961015, 4117679320, 2969657708, 1647728401, 4168645754, 1334625769),
			bci(806206, 806273, 806182, 806254, 806158, 806323, 806263, 806295, 158358)},
		{"900KB1", genPredictableRandomData(900 * 1024), "-1",
			1402104902,
			bc(984137596, 1527206082, 1102975844, 1428961015, 4117679320, 2969657708, 1647728401, 4168645754, 360300138, 4141343228),
			bci(806206, 806273, 806182, 806254, 806158, 806323, 806263, 806295, 806166, 177790)},
	} {
		filename := filepath.Join(tmpdir, tc.name)
		rd, err := createBzipFile(filename, tc.blockSize, tc.data)
		if err != nil {
			t.Fatalf("createBzipFile: %v", err)
		}
		defer rd.Close()
		sc := NewScanner(rd)
		var data []byte
		n := 0
		var (
			pwg    sync.WaitGroup
			pbuf   []byte
			perr   error
			prgCh  = make(chan Progress, 3)
			prgwg  sync.WaitGroup
			prgerr error
		)
		prgwg.Add(1)
		go func(n string) {
			next := uint64(1)
			var err error
			for p := range prgCh {
				fmt.Printf("%#v\n", p)
				if p.Block != next {
					err = fmt.Errorf("%v: out of sequence block %#v\n", n, p)
					break
				}
				next++
			}
			prgerr = err
			prgwg.Done()
		}(tc.name)
		dc := NewDecompressor(ctx, BZConcurrency(3), BZSendUpdates(prgCh))

		pwg.Add(1)
		go func(n string) {
			pbuf, err = ioutil.ReadAll(dc)
			if err != nil {
				t.Errorf("%v: failed to read all from parallel decompressor: %v", n, err)
			}
			pwg.Done()
		}(tc.name)
		for sc.Scan(ctx) {
			block, bitOffset, blockSize, blockCRC := sc.Block()
			// Parallel decompress.
			dc.Decompress(sc.BlockSize(), block, bitOffset, blockCRC)
			if len(block) == 0 {
				continue
			}
			if got, want := blockCRC, tc.blockCRCs[n]; got != want {
				t.Errorf("%v: got %v, want %v", tc.name, got, want)
			}
			if got, want := blockSize, tc.blockSizes[n]; got != want {
				t.Errorf("%v: got %v, want %v", tc.name, got, want)
			}
			// Synchronous scan + decompress.
			rd := bzip2.NewBlockReader(sc.BlockSize(), block, bitOffset)
			buf, err := ioutil.ReadAll(rd)
			if err != nil {
				t.Errorf("%v: decompression failed: %v", tc.name, err)
			}
			data = append(data, buf...)
			n++
		}
		if err := sc.Err(); err != nil {
			t.Errorf("%v: scan failed: %v", tc.name, err)
			continue
		}
		if got, want := sc.StreamCRC(), tc.streamCRC; got != want {
			t.Errorf("%v: got %v, want %v", tc.name, got, want)
		}
		if got, want := n, len(tc.blockSizes); got != want {
			t.Errorf("%v: got %v, want %v", tc.name, got, want)
		}
		firstN := func(n int, b []byte) []byte {
			if len(b) > n {
				return b[:n]
			}
			return b
		}
		if got, want := data, tc.data; !bytes.Equal(got, want) {
			t.Errorf("%v: got %v..., want %v...", tc.name, firstN(10, got), firstN(10, want))
		}

		{
			// Test against stdlib bzip2.
			f, err := os.Open(filename + ".bz2")
			if err != nil {
				t.Fatal(err)
			}
			bdc := gobzip2.NewReader(f)
			buf, err := ioutil.ReadAll(bdc)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := data, buf; !bytes.Equal(got, want) {
				t.Errorf("%v: got %v..., want %v...", tc.name, firstN(10, got), firstN(10, want))
			}
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
			t.Errorf("failed to read from paralle decompressor: %v", err)
		}
		close(prgCh)
		if got, want := pbuf, tc.data; !bytes.Equal(got, want) {
			t.Errorf("%v: got %v..., want %v...", tc.name, firstN(10, got), firstN(10, want))
		}
		prgwg.Wait()
		if err := prgerr; err != nil {
			t.Errorf("progress indicator error: %v", err)
		}
	}
}
