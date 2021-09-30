package main_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cosnicolaou/pbzip2/internal"
)

func pbzipCmd(filename string) ([]byte, error) {
	ifile := filename + ".bz2"
	ofile := filename + ".test"
	cmd := exec.Command("go", "run", ".",
		"--input="+ifile,
		"--output="+ofile,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to run pbzip2: %s: %s", strings.Join(cmd.Args, " "), output)
	}
	return os.ReadFile(ofile)
}

func TestCmd(t *testing.T) {
	tmpdir := t.TempDir()
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"800KB1", internal.GenPredictableRandomData(800 * 1024)},
	} {
		filename := filepath.Join(tmpdir, tc.name)
		if err := internal.CreateBzipFile(filename, "-3", tc.data); err != nil {
			t.Fatalf("%v: %v", tc.name, err)
		}
		data, err := pbzipCmd(filename)
		if err != nil {
			t.Fatalf("%v: %v", tc.name, err)
		}
		if got, want := data, tc.data; !bytes.Equal(got, want) {
			t.Errorf("%v: got %v, want %v", tc.name, internal.FirstN(20, got), internal.FirstN(20, want))
		}
	}
}
