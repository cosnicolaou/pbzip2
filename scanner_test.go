package pbzip2

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
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
		{"hello", []byte("hello world\n"), "-1", 1324148790, bc(1324148790), bci(253)},
		{"100KB1", genPredictableRandomData(100 * 1024), "-1", 2846214228, bc(984137596, 3707025068), bci(806206, 22712)},
		{"300KB1", genPredictableRandomData(300 * 1024), "-1", 2560071082,
			bc(984137596, 1527206082, 1102975844, 2729642890),
			bci(806206, 806273, 806182, 61754)},
		{"400KB1", genPredictableRandomData(400 * 1024), "-1", 182711008,
			bc(984137596, 1527206082, 1102975844, 1428961015, 3572671310),
			bci(806206, 806273, 806182, 806254, 81086)},
	} {
		rd, err := createBzipFile(tc.name, tc.blockSize, tc.data)
		if err != nil {
			t.Fatalf("createBzipFile: %v", err)
		}
		defer rd.Close()
		sc := NewScanner(rd)
		var crcs []uint32
		var sizes []int
		for sc.Scan() {
			block, bitOffset, blockSize := sc.Block()
			if len(block) == 0 {
				continue
			}
			tmp := make([]byte, 5)
			copy(tmp, block[:5])
			//fmt.Printf("SH: %08b offset %v\n", tmp, bitOffset)
			for i := 8; i > bitOffset; i-- {
				tmp = bitstreamShift(tmp)
				//fmt.Printf("SH: %08b\n", tmp)
			}
			fmt.Printf("CRC: %02x (offset: %v)\n", tmp, bitOffset)
			crcs = append(crcs, binary.BigEndian.Uint32(tmp[1:5]))
			sizes = append(sizes, blockSize)
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

	}

}
