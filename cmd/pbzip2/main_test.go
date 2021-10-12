// Copyright 2021 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.
package main_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cosnicolaou/pbzip2/internal"
)

func pbzipCmd(filename string) ([]byte, string, error) {
	ifile := filename + ".bz2"
	ofile := filename + ".test"
	cmd := exec.Command("go", "run", ".", "unzip",
		"--output="+ofile, ifile,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, string(output), err
	}
	data, err := os.ReadFile(ofile)
	return data, string(output), err
}

func TestCmd(t *testing.T) {
	tmpdir := t.TempDir()
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"800KB1", internal.GenReproducibleRandomData(800 * 1024)},
	} {
		filename := filepath.Join(tmpdir, tc.name)
		if err := internal.CreateBzipFile(filename, "-3", tc.data); err != nil {
			t.Fatalf("%v: %v", tc.name, err)
		}
		data, out, err := pbzipCmd(filename)
		if err != nil {
			t.Fatalf("%v: %v: %v", tc.name, out, err)
		}
		if got, want := data, tc.data; !bytes.Equal(got, want) {
			t.Errorf("%v: got %v, want %v", tc.name, internal.FirstN(20, got), internal.FirstN(20, want))
		}
	}
}

func TestErrors(t *testing.T) {
	tmpdir := t.TempDir()

	empty := filepath.Join(tmpdir, "empty")
	if err := os.WriteFile(empty+".bz2", nil, 0600); err != nil {
		t.Fatal(err)
	}
	_, out, err := pbzipCmd(empty)
	if err == nil || !strings.Contains(out, "failed to read stream header: EOF") {
		t.Fatalf("missing or wrong error message: %v: %v", out, err)
	}

	hello := filepath.Join(tmpdir, "hello")

	if err := internal.CreateBzipFile(hello, "-1", []byte("hello world\n")); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(hello + ".bz2")
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] = 0x0

	corrupt := hello + "-corrupt"
	if err := os.WriteFile(corrupt+".bz2", data, 0600); err != nil {
		t.Fatal(err)
	}

	_, out, err = pbzipCmd(corrupt)
	if err == nil || !strings.Contains(out, "mismatched stream CRCs") {
		t.Fatalf("missing or wrong error message: %v: %v", out, err)
	}
}
