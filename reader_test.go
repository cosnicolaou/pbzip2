// Copyright 2021 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package pbzip2_test

import (
	"bytes"
	"compress/bzip2"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/cosnicolaou/pbzip2"
	"github.com/cosnicolaou/pbzip2/internal"
	ibzip2 "github.com/cosnicolaou/pbzip2/internal/bzip2"
)

func ExampleReader() {
	input, err := os.Open(filepath.Join("testdata", "hello_world.bz2"))
	if err != nil {
		panic(err)
	}
	io.Copy(os.Stdout, bzip2.NewReader(input))
	// Output:
	// hello world
}

// readAllSample is like os.ReadAll except that it samples the number of
// goroutines that are currently being used for decompression.
func readAllSample(r io.Reader) ([]byte, int64, error) {
	var max int64
	b := make([]byte, 0, 512)
	for {
		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
		n, err := r.Read(b[len(b):cap(b)])
		tmp := pbzip2.GetNumDecompressionGoRoutines()
		if tmp > max {
			max = tmp
		}
		b = b[:len(b)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return b, max, err
		}
	}
}

func validateGoRoutines(t *testing.T, start, stop, max int64, concurrency int) {
	_, _, line, _ := runtime.Caller(1)
	if max <= start {
		t.Errorf("line %v: concurrency: %v, suspicious go routine accounting", line, concurrency)
	}
	t.Logf("max goroutines: %v", max)
	if got, want := stop, start; got != want {
		t.Errorf("line %v: concurrency: %v, goroutine leak: %v %v", line, concurrency, got, want)
	}
}

func TestIOReader(t *testing.T) {
	var maxDecGoroutines int64
	ngs := pbzip2.GetNumDecompressionGoRoutines()

	testIOReader(t, func(rd io.Reader) ([]byte, error) {
		n, max, err := readAllSample(rd)
		maxDecGoroutines = max
		return n, err
	})

	validateGoRoutines(t,
		ngs,
		pbzip2.GetNumDecompressionGoRoutines(),
		maxDecGoroutines,
		-1)
}

func testIOReader(t *testing.T, readAll func(io.Reader) ([]byte, error)) {
	ctx := context.Background()

	for _, name := range []string{"empty", "hello", "300KB3_Random", "900KB2_Random", "1033KB4_Random"} {
		filename := bzip2Files[name]
		stdlibData := readBzipFile(t, filename)

		for _, concurrency := range []int{1, 2, runtime.GOMAXPROCS(-1)} {
			rd := openBzipFile(t, filename)
			drd := pbzip2.NewReader(ctx, rd,
				pbzip2.DecompressionOptions(pbzip2.BZConcurrency(concurrency)))
			data, err := readAll(drd)
			if err != nil {
				t.Errorf("%v: readAll failed: %v", name, err)
			}

			if got, want := data, data; !bytes.Equal(got, want) {
				t.Errorf("%v: got %v..., want %v...", name, internal.FirstN(10, got), internal.FirstN(10, want))
			}

			if got, want := data, stdlibData; !bytes.Equal(got, want) {
				t.Errorf("%v: got %v..., want %v...", name, internal.FirstN(10, got), internal.FirstN(10, want))
			}
			rd.Close()
		}
	}
}

// readAllSampleAndCancel is like os.ReadAll except that it samples the number
// of goroutines that are currently being used for decompression and also
// calls the cancel function after a specified number of iterations.
func readAllSampleAndCancel(cancel func(), when int, r io.Reader) ([]byte, int64, error) {
	var max int64
	b := make([]byte, 0, 64)
	i := 0
	for {
		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
		n, err := r.Read(b[len(b):cap(b)])
		tmp := pbzip2.GetNumDecompressionGoRoutines()
		if tmp > max {
			max = tmp
		}
		b = b[:len(b)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return b, max, err
		}
		i++
		if i > when {
			cancel()
		}
	}
}

func TestCancelation(t *testing.T) {
	ctx := context.Background()

	filename := bzip2Files["1033KB4_Random"]

	ngs := pbzip2.GetNumDecompressionGoRoutines()

	// Test with different levels of concurrency.
	for _, concurrency := range []int{1, 2, runtime.GOMAXPROCS(-1)} {
		dcOpts := pbzip2.DecompressionOptions(pbzip2.BZConcurrency(concurrency))

		for i := range []int{1, 77, 100} {
			rd := openBzipFile(t, filename)
			ctx, cancel := context.WithCancel(ctx)
			drd := pbzip2.NewReader(ctx, rd, dcOpts)

			_, max, err := readAllSampleAndCancel(cancel, i, drd)

			validateGoRoutines(t,
				ngs,
				pbzip2.GetNumDecompressionGoRoutines(),
				max,
				concurrency)

			if err == nil || err.Error() != "context canceled" {
				t.Errorf("expected an error or different error to the one received: %v", err)
			}
			cancel()
		}
	}

	// Test immediate cancelation.
	rd := openBzipFile(t, filename)
	ctx, cancel := context.WithCancel(ctx)
	drd := pbzip2.NewReader(ctx, rd)
	cancel()
	_, err := io.ReadAll(drd)
	if err == nil || err.Error() != "context canceled" {
		t.Errorf("expected an error or different error to the one received: %v", err)
	}

}

func TestReaderErrors(t *testing.T) {
	ctx := context.Background()
	rd := bytes.NewBuffer(nil)
	drd := pbzip2.NewReader(ctx, rd)
	_, err := io.ReadAll(drd)
	if err == nil || err.Error() != "failed to read stream header: EOF" {
		t.Errorf("expected an error or different error to the one received: %v", err)
	}

	testError := func(buf []byte, msg string) {
		rd := bytes.NewBuffer(buf)
		drd := pbzip2.NewReader(ctx, rd)
		_, err = io.ReadAll(drd)
		if err == nil || !strings.Contains(err.Error(), msg) {
			_, _, line, _ := runtime.Caller(1)
			t.Errorf("line: %v expected an error or different error to the one received: %v", line, err)
		}
	}

	drd = pbzip2.NewReader(ctx, &errorReader{})
	_, err = io.ReadAll(drd)
	if err == nil || !strings.Contains(err.Error(), "failed to read stream header: oops") {
		t.Errorf("expected an error or different error to the one received: %v", err)
	}

	testError([]byte{0x1, 0x1, 0x1}, "stream header is too small")

	buf, l := readFile(t, "hello")
	buf[l] = 0x1
	buf[l-1] = 0x1
	testError(buf, "mismatched stream CRCs")

	buf, l = readFile(t, "hello")
	buf[l-4] = 0x1
	testError(buf, "failed to find trailer")

	buf, _ = readFile(t, "hello")
	buf[0] = 0x1
	testError(buf, "wrong file magic: 015a")

	buf, _ = readFile(t, "hello")
	buf[2] = 0x1
	testError(buf, "wrong version")

	buf, _ = readFile(t, "hello")
	buf[3] = 0x1
	testError(buf, "bad block size")

	buf, _ = readFile(t, "300KB1")
	corrupted := buf[:9000]
	corrupted = append(corrupted, ibzip2.BlockMagic[:]...)
	corrupted = append(corrupted, buf[9000:]...)
	testError(corrupted, "bzip2 data invalid: data exceeds block size")
}

type errorReader struct{}

func (er *errorReader) Read(buf []byte) (int, error) {
	return 1, fmt.Errorf("oops")
}
