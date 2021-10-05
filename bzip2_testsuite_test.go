// Copyright 2021 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.
package pbzip2_test

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/cosnicolaou/pbzip2"
)

func gitcloneTestsuite(tmpdir string) error {
	opts := []string{"clone"}
	if runtime.GOOS == "windows" {
		opts = append(opts, "--config", "core.autocrlf=input")
	}
	opts = append(opts, "https://sourceware.org/git/bzip2-tests.git")
	cmd := exec.Command("git", opts...)
	cmd.Dir = tmpdir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %s: %v", strings.Join(cmd.Args, " "), out, err)
	}
	return nil
}

type testfile struct {
	filename string
	md5      string
	err      string
}

// Note the the current implementation does not correctly handle concatenated
// files nor multiple bzip streams within a single file.
func getBzip2Files(tmpdir string) ([]testfile, error) {
	var exceptions = map[string]string{
		"lbzip2/concat.bz2":             "mismatched CRCs: 451583718 != 2325361207",
		"lbzip2/gap.bz2":                "mismatched CRCs: 904657248 != 1209588216",
		"lbzip2/rand.bz2":               "bzip2 data invalid: deprecated randomized files",
		"lbzip2/trash.bz2":              "failed to find trailer",
		"commons-compress/multiple.bz2": "mismatched CRCs: 349224370 != 670534500",
	}

	files := map[string]bool{}
	sums := map[string]string{}
	err := filepath.Walk(tmpdir,
		func(path string, info fs.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if base := strings.TrimSuffix(info.Name(), ".bz2"); base != info.Name() {
				files[path] = true
			}
			if base := strings.TrimSuffix(info.Name(), ".md5"); base != info.Name() {
				buf, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				dirname := filepath.Dir(path)
				sums[filepath.Join(dirname, base+".bz2")] = strings.TrimSuffix(string(buf), "  -\n")
			}
			return nil
		})

	tldir := filepath.Join(tmpdir, "bzip2-tests") + string(filepath.Separator)

	pairs := make([]testfile, 0, len(files))
	for file := range files {
		err := exceptions[strings.TrimPrefix(file, tldir)]
		pairs = append(pairs, testfile{filename: file, err: err, md5: sums[file]})
	}
	return pairs, err
}

func TestBzip2Tests(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()

	tmpdir = "./foo"
	if err := gitcloneTestsuite(tmpdir); err != nil {
		t.Fatal(err)
	}

	testcases, err := getBzip2Files(tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range testcases {
		bzfile, err := os.Open(tc.filename)
		if err != nil {
			t.Errorf("%v: %v", tc.filename, err)
		}
		rd := pbzip2.NewReader(ctx, bzfile)
		h := md5.New()
		_, err = io.Copy(h, rd)
		if len(tc.err) > 0 {
			if err == nil || err.Error() != tc.err {
				t.Errorf("%v: missing or wrong error: got %v: want: %v", tc.filename, err, tc.err)
			}
			continue
		} else if err != nil {
			t.Errorf("%v: %v", tc.filename, err)
		}
		if got, want := fmt.Sprintf("%02x", h.Sum(nil)), tc.md5; got != want {
			t.Errorf("%v: got %v, want %v", tc.filename, got, want)
		}
	}
}
