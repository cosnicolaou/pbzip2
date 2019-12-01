package pbzip2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

func TestBitstreamShift(t *testing.T) {
	b := func(b ...byte) []byte {
		return b
	}
	for i, tc := range []struct {
		i, o []byte
	}{
		{b(0x00, 0x00, 0x00, 0x00, 0x00, 0x00), b(0x00, 0x00, 0x00, 0x00, 0x00, 0x00)},
		{b(0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF), b(0x00, 0x00, 0x00, 0x00, 0x7F, 0xFF)},
		{b(0x80, 0x80, 0x80, 0x80, 0x80, 0x80), b(0x40, 0x40, 0x40, 0x40, 0x40, 0x40)},
		{b(0x11, 0x11, 0x11, 0x11, 0x11, 0x11), b(0x08, 0x88, 0x88, 0x88, 0x88, 0x88)},
		{b(0x80, 0x00, 0x00, 0x00, 0x00, 0x00), b(0x40, 0x00, 0x00, 0x00, 0x00, 0x00)},
		{b(0x80, 0x00, 0x00, 0x00, 0x00, 0xFF), b(0x40, 0x00, 0x00, 0x00, 0x00, 0x7F)},
		{b(0b00000000, 0b00110001, 0b10011010, 0b11001010, 0b11111111, 0b11111111),
			b(0b00000000, 0b00011000, 0b11001101, 0b01100101, 0b01111111, 0b11111111)},
	} {
		if got, want := bitstreamShift(tc.i), tc.o; !bytes.Equal(got[:], want[:]) {
			t.Errorf("%v: got %08b, want %08b", i, got, want)

		}
	}
}

// mapToBytes maps the bit values into byte values so that we can
// use bytes.Index to find subpatterns.
func mapToBytes(in []byte) (out []byte) {
	out = make([]byte, len(in)*8)
	for i := 0; i < len(in); i++ {
		for j := 0; j < 8; j++ {
			out[(i*8)+j] = (in[i] & (1 << (7 - j))) >> (7 - j)
		}
	}
	return
}

func prbits(in []byte) string {
	var out strings.Builder
	for _, v := range in {
		out.WriteString(fmt.Sprintf("%x ", v))
	}
	return out.String()
}

func TestBitPatterns(t *testing.T) {
	m0, m1, m2, m3, m4, m5 := bzip2BlockMagic[0], bzip2BlockMagic[1], bzip2BlockMagic[2], bzip2BlockMagic[3], bzip2BlockMagic[4], bzip2BlockMagic[5]

	Init()
	// Find the appropriate prefix of the first 4 bytes magic # in the
	// lookup table for the first 4 bytes. The magic number must appear
	// as a suffix (truncated to 4 bytes) in the bit patterns represented
	// by the first lookup table.
	magic := mapToBytes([]byte{m0, m1, m2, m3})
	for p, s := range firstBlockMagicLookup {
		bits := [4]byte{}
		binary.LittleEndian.PutUint32(bits[:], p)
		expanded := mapToBytes(bits[:])
		// 32-s truncats the magic number to the 4 byte boundary.
		pos := bytes.Index(expanded[:], magic[:32-s])
		if got, want := pos, s; got != int(want) {
			t.Errorf("got %v, want %v\n", got, want)
		}
	}

	// The second lookup table overlaps with first, with the top most byte
	// of the first 4 bytes of the magic number being shifted into the
	// upper 4 bytes. Therefore, the prefix is the bits shifted from the
	// the 4th byte of the lower 4 bytes of magic, plus the 5th and 6th bytes
	// of the magic number,
	magic = mapToBytes([]byte{m3, m4, m5, 0})
	for p, s := range secondBlockMagicLookup {
		bits := [4]byte{}
		binary.LittleEndian.PutUint32(bits[:], p)
		expanded := mapToBytes(bits[:])
		from := 8 - s       // the number of bits remaining after the shift
		to := from + 16 + s // the total size of the prefix, plus the shift offset
		pos := bytes.Index(expanded[:], magic[from:to])
		if got, want := pos, 0; got != int(want) {
			t.Errorf("got %v, want %v\n", got, want)
			t.FailNow()
		}
	}
}

func insertMagic(buf, magic []byte, p int) []byte {
	bytePos := p / 8
	bitPos := p % 8
	if bytePos > len(buf) {
		return nil
	}
	save := buf[bytePos]
	copy(buf[bytePos:], magic)
	if bitPos == 0 {
		return buf
	}
	tail := buf[bytePos:]
	for i := 1; i <= bitPos; i++ {
		tail = bitstreamShift(tail)
	}
	copy(buf[bytePos:], tail)
	//	fmt.Printf("BP: %v -> %v .. %v %08b .. %08b\n", p, bytePos, bitPos, (uint8(0xff) << (8 - bitPos)), (0xff >> bitPos))
	buf[bytePos] = save&(uint8(0xff)<<(8-bitPos)) | (buf[bytePos] & (0xff >> bitPos))
	return buf
}

func TestFindPatterns(t *testing.T) {
	Init()
	shifted := func(shift int) []byte {
		buf := make([]byte, len(bzip2BlockMagic)+1)
		copy(buf, bzip2BlockMagic[:])
		for i := 0; i < shift; i++ {
			buf = bitstreamShift(buf)
		}
		return buf
	}
	for _, tc := range []struct {
		buf                   []byte
		byteOffset, bitOffset int
	}{
		{bzip2BlockMagic[:], 0, 0},
		{append(bzip2BlockMagic[:], 0x11), 0, 0},
		{append(bzip2BlockMagic[:], 0xda, 0x4b, 0xd0, 0xce), 0, 0},
		{append([]byte{0x0}, bzip2BlockMagic[:]...), 1, 0},
		{append([]byte{0x0}, shifted(1)...), 1, 1},
		{append([]byte{0x0}, shifted(2)...), 1, 2},
		{append([]byte{0x0}, shifted(6)...), 1, 6},
	} {
		byteOffset, bitOffset := scanBitStream(firstBlockMagicLookup, secondBlockMagicLookup, tc.buf)
		if got, want := byteOffset, tc.byteOffset; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := bitOffset, tc.bitOffset; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 6; i < 65; i++ {
		filler := make([]byte, i)
		n, err := rnd.Read(filler)
		if err != nil {
			t.Errorf("%v: failed to %v rand bytes", i, err)
			continue
		}
		if got, want := n, i; got != want {
			t.Errorf("%v: got %v, want %v", i, got, want)
			continue
		}
		for p := 0; p < (i-6)*8; p++ {
			// fill the buffer with rand numbers before inserting the bit
			// aligned magic number we want to find.
			buf := make([]byte, i)
			copy(buf, filler)
			m := insertMagic(buf, bzip2BlockMagic[:], p)
			byteOffset, bitOffset := scanBitStream(firstBlockMagicLookup, secondBlockMagicLookup, m)
			if got, want := byteOffset, p/8; got != want {
				t.Fatalf("%v: %v: got %v, want %v", i, p, got, want)
			}
			if got, want := bitOffset, p%8; got != want {
				t.Errorf("%v: %v: got %v, want %v", i, p, got, want)
			}
		}
	}
}

func TestFindTrailer(t *testing.T) {
	crc := []byte{0x01, 0x02, 0x03, 0x04}
	end := 10
	for i := 0; i < 8; i++ {
		buf := make([]byte, 6+4+1)
		copy(buf, bzip2EOSMagic[:])
		copy(buf[6:], crc)
		for s := 0; s < i; s++ {
			buf = bitstreamShift(buf)
		}
		found, length, offset := findTrailingMagicAndCRC(buf[:end], bzip2EOSMagic[:])
		if got, want := found, crc; !bytes.Equal(got, want) {
			t.Errorf("%v: got: %02x, want %02x\n", i, got, want)
		}
		if got, want := length, 10; got != want {
			t.Errorf("%v: got: %02x, want %02x\n", i, got, want)
		}
		if got, want := offset, i; got != want {
			t.Errorf("%v: got: %02x, want %02x\n", i, got, want)
		}
		end = 11
	}
}
