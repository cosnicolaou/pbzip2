// Copyright 2020 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.
package bitstream

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cosnicolaou/pbzip2/internal/bzip2"
)

func TestBitShift(t *testing.T) {
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
		cpy := make([]byte, len(tc.i))
		copy(cpy, tc.i)
		if got, want := ShiftRight(cpy), tc.o; !bytes.Equal(got, want) {
			t.Logf("got: %v", prbits(got))
			t.Logf("want: %v", prbits(want))
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
	m0, m1, m2, m3, m4, m5 := bzip2.BlockMagic[0], bzip2.BlockMagic[1], bzip2.BlockMagic[2], bzip2.BlockMagic[3], bzip2.BlockMagic[4], bzip2.BlockMagic[5]

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
		pos := bytes.Index(expanded, magic[:32-s])
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
		pos := bytes.Index(expanded, magic[from:to])
		if got, want := pos, 0; got != want {
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
		tail = ShiftRight(tail)
	}
	copy(buf[bytePos:], tail)
	buf[bytePos] = save&(uint8(0xff)<<(8-bitPos)) | (buf[bytePos] & (0xff >> bitPos))
	return buf
}

func shifted(shift int) []byte {
	buf := make([]byte, len(bzip2.BlockMagic)+1)
	copy(buf, bzip2.BlockMagic[:])
	for i := 0; i < shift; i++ {
		buf = ShiftRight(buf)
	}
	return buf
}
func TestFindPatterns(t *testing.T) {
	Init()
	for i, tc := range []struct {
		buf                   []byte
		byteOffset, bitOffset int
	}{
		{bzip2.BlockMagic[:], 0, 0},
		{append(bzip2.BlockMagic[:], 0x11), 0, 0},
		{append(bzip2.BlockMagic[:], 0x26), 0, 0},
		{append(bzip2.BlockMagic[:], 0xda, 0x4b, 0xd0, 0xce), 0, 0},
		{append([]byte{0xff}, bzip2.BlockMagic[:]...), 1, 0},
		{append([]byte{0x2b, 0xff}, bzip2.BlockMagic[:]...), 2, 0},
		{append([]byte{0x0}, bzip2.BlockMagic[:]...), 1, 0},
		{append([]byte{0x0}, shifted(1)...), 1, 1},
		{append([]byte{0x0}, shifted(2)...), 1, 2},
		{append([]byte{0x0}, shifted(6)...), 1, 6},
		{[]byte{0xa3, 0x14, 0x15, 0x92, 0x15, 0x94, 0x2b, 0xff, 0x31, 0x41, 0x59, 0x26, 0x53, 0x59}, 8, 0},
	} {
		byteOffset, bitOffset := Scan(&zero, firstBlockMagicLookup, secondBlockMagicLookup, tc.buf)
		if got, want := byteOffset, tc.byteOffset; got != want {
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
		if got, want := bitOffset, tc.bitOffset; got != want {
			t.Errorf("%d: got %v, want %v", i, got, want)
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
			m := insertMagic(buf, bzip2.BlockMagic[:], p)
			byteOffset, bitOffset := Scan(&zero, firstBlockMagicLookup, secondBlockMagicLookup, m)
			if got, want := byteOffset, p/8; got != want {
				t.Fatalf("%v: %v: got %v, want %v", i, p, got, want)
			}
			if got, want := bitOffset, p%8; got != want {
				t.Errorf("%v: %v: got %v, want %v", i, p, got, want)
			}
		}
	}

}

func TestPartialFalsePositives(t *testing.T) {
	Init()
	// partial patterns
	partial := [6][]byte{}
	for i := range partial {
		partial[i] = make([]byte, i+1)
		copy(partial[i], bzip2.BlockMagic[:i+1])
	}
	// zero out the last bit of the complete magic # so that it doesn't
	// match
	partial[5][5] &= 0xf7

	for i, p := range partial {
		byteOffset, bitOffset := Scan(&zero, firstBlockMagicLookup, secondBlockMagicLookup, p)
		if got, want := byteOffset, -1; got != want {
			t.Errorf("%v: %v: got %v, want %v", i, p, got, want)
		}
		if got, want := bitOffset, -1; got != want {
			t.Errorf("%v: %v: got %v, want %v", i, p, got, want)
		}
		tmp := make([]byte, len(p)+6)
		copy(tmp, p)
		for shift := 0; shift < 7; shift++ {
			tmp = ShiftRight(tmp)
			copy(tmp[len(tmp)-6:], bzip2.BlockMagic[:])
			byteOffset, bitOffset := Scan(&zero, firstBlockMagicLookup, secondBlockMagicLookup, tmp)
			if got, want := byteOffset, len(p); got != want {
				t.Errorf("%v: %v: got %v, want %v", i, p, got, want)
			}
			if got, want := bitOffset, 0; got != want {
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
		copy(buf, bzip2.EOSMagic[:])
		copy(buf[6:], crc)
		for s := 0; s < i; s++ {
			buf = ShiftRight(buf)
		}
		found, length, offset := FindTrailingMagicAndCRC(buf[:end], bzip2.EOSMagic[:])
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

var (
	ones = []string{
		"[00000000 11111111 11111111 11111111 00000000 00000000]",
		"[00000000 01111111 11111111 11111111 10000000 00000000]",
		"[00000000 00111111 11111111 11111111 11000000 00000000]",
		"[00000000 00011111 11111111 11111111 11100000 00000000]",
		"[00000000 00001111 11111111 11111111 11110000 00000000]",
		"[00000000 00000111 11111111 11111111 11111000 00000000]",
		"[00000000 00000011 11111111 11111111 11111100 00000000]",
		"[00000000 00000001 11111111 11111111 11111110 00000000]",
	}
	zereos = []string{
		"[11111111 00000000 00000000 00000000 11111111 11111111]",
		"[11111111 10000000 00000000 00000000 01111111 11111111]",
		"[11111111 11000000 00000000 00000000 00111111 11111111]",
		"[11111111 11100000 00000000 00000000 00011111 11111111]",
		"[11111111 11110000 00000000 00000000 00001111 11111111]",
		"[11111111 11111000 00000000 00000000 00000111 11111111]",
		"[11111111 11111100 00000000 00000000 00000011 11111111]",
		"[11111111 11111110 00000000 00000000 00000001 11111111]",
	}
)

func TestOverwriteAtOffset(t *testing.T) {
	magic := []byte{0xff, 0xff, 0xff}
	for i := 0; i < 8; i++ {
		buf := make([]byte, 6)
		OverwriteAtBitOffset(buf, 8+i, magic)
		if got, want := fmt.Sprintf("%08b", buf), ones[i]; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	magic = []byte{0x00, 0x00, 0x00}
	for i := 0; i < 8; i++ {
		buf := make([]byte, 6)
		for i := 0; i < len(buf); i++ {
			buf[i] = 0xff
		}
		OverwriteAtBitOffset(buf, 8+i, magic)
		if got, want := fmt.Sprintf("%08b", buf), zereos[i]; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestBitAppend(t *testing.T) {

	s := func(b ...byte) []byte {
		return b
	}

	for i, tc := range []struct {
		a  []byte
		al int
		b  []byte
		bo int
		bl int
		r  []byte
		rl int
	}{
		// non-byte aligned destination, byte aligned source
		{s(0xff), 8, s(0xff), 0, 8, s(0xff, 0xff), 16},
		{s(0xfe), 7, s(0xff), 0, 8, s(0xff, 0xfe), 15},
		{s(0xfc), 6, s(0xff), 0, 8, s(0xff, 0xfc), 14},
		{s(0xf8), 5, s(0xff), 0, 8, s(0xff, 0xf8), 13},
		{s(0xf0), 4, s(0xff), 0, 8, s(0xff, 0xf0), 12},
		{s(0xe0), 3, s(0xff), 0, 8, s(0xff, 0xe0), 11},
		{s(0xc0), 2, s(0xff), 0, 8, s(0xff, 0xc0), 10},
		{s(0x80), 1, s(0xff), 0, 8, s(0xff, 0x80), 9},
		{nil, 0, s(0xff), 0, 8, s(0xff), 8},

		// byte aligned destination, non byte aligned source
		{s(0xff), 8, s(0x7f), 1, 7, s(0xff, 0xfe), 15},
		{s(0xff), 8, s(0x3f), 2, 6, s(0xff, 0xfc), 14},
		{s(0xff), 8, s(0x1f), 3, 5, s(0xff, 0xf8), 13},
		{s(0xff), 8, s(0x0f), 4, 4, s(0xff, 0xf0), 12},
		{s(0xff), 8, s(0x07), 5, 3, s(0xff, 0xe0), 11},
		{s(0xff), 8, s(0x03), 6, 2, s(0xff, 0xc0), 10},
		{s(0xff), 8, s(0x01), 7, 1, s(0xff, 0x80), 9},

		// mix-and-match
		{s(0xfe), 7, s(0x7f), 1, 7, s(0xff, 0xfc), 14},
		{s(0xfe), 7, s(0x01), 7, 1, s(0xff), 8},
		{s(0xe0), 3, s(0x01, 0xff), 7, 9, s(0xff, 0xf0), 12},
		{s(0xe0), 1, s(0x01, 0xff), 7, 9, s(0xff, 0xc0), 10},
	} {
		wr := &BitWriter{}
		wr.Init(tc.a, tc.al, 0)
		wr.Append(tc.b, tc.bo, tc.bl)
		r, rl := wr.Data()
		if got, want := r, tc.r; !bytes.Equal(got, want) {
			fmt.Printf("LEN: %v . %v .. %08b %08b\n", len(r), len(tc.r), got, want)
			t.Errorf("%v: got %08b, want %08b", i, got, want)
			break
		}
		if got, want := rl, tc.rl; got != want {
			t.Errorf("%v: got %08b, want %08b", i, got, want)
			break
		}
	}

}
