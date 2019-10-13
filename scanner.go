package jsonutil

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
)

type scannerOpts struct {
	buf         []byte
	max         int
	initialSize int
}

// ScannerOption represenst an option to NewBZ2BlockScanner.
type ScannerOption func(*scannerOpts)

// ScannerBufferOption specifies the buffer and max size (see bufio.Scanner.Buffer)
// to use with the underlying scanner.
func ScannerBufferOption(buf []byte, max int) ScannerOption {
	return func(o *scannerOpts) {
		o.buf = buf
		o.max = max
	}
}

// ScannerInitialSampleSize sets the initial size of the slice used
// to record the size of each scanned line. Set it to any non-zero value
// to enable recording of the sizes of the inputs.
func ScannerInitialSampleSize(max int) ScannerOption {
	return func(o *scannerOpts) {
		o.initialSize = max
	}
}


// See https://en.wikipedia.org/wiki/Bzip2 for an explanation of the file
// format.
var bzip2FileMagic = []byte{0x42, 0x5a} // "BZ"

var bzip2BlockMagic = []byte{0x31, 0x41, 0x59, 0x26, 0x53, 0x59}

var bzip2EOSMagic = []byte{0x17, 0x72, 0x45, 0x38, 0x50, 0x90}

func (sc *BZ2BlockScanner) blockSplit(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	fmt.Printf("A.. %v\n", len(data))
	if i := bytes.Index(data, bzip2BlockMagic); i >= 0 {
		return i + len(bzip2BlockMagic), data[:i], nil
	}
	if i := bytes.Index(data, bzip2EOSMagic); i >= 0 {
		fmt.Printf("FOUND EOS AT.. %v\n", i)
		return i + len(bzip2EOSMagic), data[:i], nil
	}
	return 0, nil, nil
}

// BZ2BlockScanner is a quick-and-dirty implementation of a scanner that
// returns entire bz2 blocks. It works by splitting the input into
// blocks terminated by either the bz2 block magic or bz2 end of stream
// magic number sequences as documented in https://en.wikipedia.org/wiki/Bzip2.
// The first block discovered will be the stream header and this
// is validated and consumed.
type BZ2BlockScanner struct {
	sc     *bufio.Scanner
	err    error
	first  bool
	header [4]byte
	max    int
	sizes  []float64
}

// NewBZ2BlockScanner returns a new instance of BZ2BlockScanner.
func NewBZ2BlockScanner(rd io.Reader, opts ...ScannerOption) *BZ2BlockScanner {
	scopts := scannerOpts{
		buf: make([]byte, 0, 10*1024*1024),
		max: 10 * 1024 * 1024,
	}
	for _, fn := range opts {
		fn(&scopts)
	}
	underlying := bufio.NewScanner(rd)
	underlying.Buffer(scopts.buf, scopts.max)
	bzs := &BZ2BlockScanner{
		sc:    underlying,
		first: true,
	}
	if s := scopts.initialSize; s > 0 {
		bzs.sizes = make([]float64, 0, s)
	}
	underlying.Split(bzs.blockSplit)
	return bzs
}

func (sc *BZ2BlockScanner) scanHeader() bool {
	if !sc.first {
		return true
	}
	if !sc.sc.Scan() {
		if err := sc.sc.Err(); err != nil {
			sc.err = err
			return false
		}
		sc.err = fmt.Errorf("failed to find stream header")
		return false
	}
	sc.first = false
	// Validate header.
	//	.magic:16              = 'BZ' signature/magic number
	//	.version:8             = 'h' for Bzip2 ('H'uffman coding),
	//                           '0' for //Bzip1 (deprecated)
	//	.hundred_k_blocksize:8 = '1'..'9' block-size 100 kB-900 kB
	//                           (uncompressed)
	header := sc.sc.Bytes()
	if len(header) != 4 {
		sc.err = fmt.Errorf("stream header is the wrong size: %v", len(header))
		return false
	}
	if !bytes.Equal(header[0:2], bzip2FileMagic) {
		sc.err = fmt.Errorf("wrong file magic: %x", header[0:2])
		return false
	}
	if header[2] != 'h' {
		sc.err = fmt.Errorf("wrong version: %c", header[2])
		return false
	}
	if s := header[3]; s < '0' || s > '9' {
		sc.err = fmt.Errorf("bad block size: %c", s)
		return false
	}
	copy(sc.header[:], header)
	return true
}

// Scan returns true if there a block to be returned.
func (sc *BZ2BlockScanner) Scan() bool {
	if sc.err != nil {
		return false
	}
	if !sc.scanHeader() {
		return false
	}
	if sc.sc.Scan() {
		l := len(sc.sc.Bytes())
		if l > sc.max {
			sc.max = l
		}
		if sc.sizes != nil {
			sc.sizes = append(sc.sizes, float64(l))
		}
		return true
	}
	return false
}

// Sizes returns a slice of the sizes of each input line. It is returned
// as a float64 to simplify using it with various stats packages. The max
// size is tracked also.
func (sc *BZ2BlockScanner) Sizes() ([]float64, int) {
	return nil, sc.max
}

// StreamHeader returns the stream header. It can only
// be called after Scan has been called at least once successfully.
func (sc *BZ2BlockScanner) StreamHeader() []byte {
	if sc.first {
		return nil
	}
	return sc.header[:]
}

// Block returns the currently scanned block. It is returned
// as a copy of the underlying data to allow for concurrent use
// and includes the magic number.
func (sc *BZ2BlockScanner) Block() []byte {
	bl := sc.sc.Bytes()
	block := make([]byte, len(bl)+len(bzip2BlockMagic))
	copy(block[:len(bzip2BlockMagic)], bzip2BlockMagic)
	copy(block[len(bzip2BlockMagic):], bl)
	return block
}

// Err returns any error encountered by the scanner.
func (sc *BZ2BlockScanner) Err() error {
	if sc.err != nil {
		return sc.err
	}
	return sc.sc.Err()
}
