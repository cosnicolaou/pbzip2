// Copyright 2020 Cosmos Nicolaou. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.
package pbzip2

import (
	"bytes"
	"encoding/binary"
	"log"
)

// NOTE: bzip2 bitstreams are created by packing 8 bits into a byte with
//       the most signifiant bit being the first bit, that is, it the bitstream
//       can be visualized as flowing from left to right.

// bitstreamShift shifts the contents of a  byte slice, with carry, one position
// to the right. The carry is from the least significant bit to the most significant.
func bitstreamShift(input []byte) []byte {
	for pos := len(input) - 1; pos >= 1; pos-- {
		input[pos] >>= 1
		input[pos] = (input[pos] & 0x7f) | (input[pos-1] & 0x1 << 7)
	}
	input[0] >>= 1
	return input
}

// allShiftedValues generate a lookup table used to find bit aligned
// patterns in a byte stream. That is, for any n-bit pattern that can
// occur in any position in a bit stream, generate all possible byte
// sequences that can contain it. Using a 4 bit pattern, PPPP, as an example:
// PPPPPbbbb, bPPPPPbb, bbPPPPPb, where PPPPP is the fixed pattern
// and b is all possible combinations of 0 and 1. The returned lookup
// table returns the bit offset of the pattern in the byte stream.
//
// allShiftedValues is not a general implementation and is customised
// a 6 byte pattern that is mapped to two uint32 for faster loading
// and comparison. Logically it operates as follows, but produces
// two 32 bit lookup tables rather than one 64bit to reduce the memory and
// CPU cost of generating them.
// a. fill out all possible values for the trailing two bytes.
// b. shift the 6 bytes, one bit at a time, to the right in the bit stream,
//    for two bytes.
func allShiftedValues(magic [6]byte) (firstWordMap map[uint32]uint8, secondWordMap map[uint32]uint8) {
	m0, m1, m2, m3, m4, m5 := magic[0], magic[1], magic[2], magic[3], magic[4], magic[5]

	// lookup table for second uint32 which is composed of the last two bytes
	// of the magic number shifted to the right 8 times and all possible
	// values filled in.
	secondWordMap = make(map[uint32]uint8, 256*256*8)
	first, second := make([]byte, 6), make([]byte, 6)
	for i := 0; i < 256; i++ {
		for j := 0; j < 256; j++ {
			second[0] = 0x0
			second[1] = m3
			second[2] = m4
			second[3] = m5
			second[4] = uint8(i)
			second[5] = uint8(j)
			secondWordMap[binary.LittleEndian.Uint32(second[2:])] = 0
			// shift right 8 times.
			for s := 1; s < 8; s++ {
				second = bitstreamShift(second)
				secondWordMap[binary.LittleEndian.Uint32(second[2:])] = uint8(s)
			}
		}
	}

	// lookup table for the first 4 bytes of the magic number which can
	// be shifted left 7 times with all possible values filled in for
	// the bits vacated by the shift.
	firstWordMap = make(map[uint32]uint8, (128*2)+1)
	first[0] = m0
	first[1] = m1
	first[2] = m2
	first[3] = m3
	firstWordMap[binary.LittleEndian.Uint32(first[:4])] = 0
	to := 2
	mask := uint8(0xff)
	for shift := uint8(1); shift <= 7; shift++ {
		first = bitstreamShift(first)
		mask >>= 1
		for j := 0; j < to; j++ {
			first[0] = (first[0] & mask) | (byte(j) << (8 - shift))
			firstWordMap[binary.LittleEndian.Uint32(first[:4])] = shift
		}
		to = to << 1
	}
	return
}

// scanBitStream returns the first occurrence of the pattern matched by
// the two lookup tables, in its input treating that input as a bitstream.
// It returns the offset of the byte containing the first byte of the
// pattern and the bit offset in that byte that the pattern starts at.
// That is, if the pattern occurs in the third byte, the byte offset will be
// two. If the pattern starts at the 2nd bit in the third byte, the byte offset
// is still two, and the bit offset will be 2.
func scanBitStream(first, second map[uint32]uint8, input []byte) (int, int) {
	pos := 0
	il := len(input)
	for {
		rpos := pos
		if pos+4 > il {
			break
		}
		lv := binary.LittleEndian.Uint32(input[pos : pos+4])
		shift, ok := first[lv]
		if !ok {
			pos++
			continue
		}
		/*
			fmt.Printf("OK: %v in %v\n", pos, shift)
			cpy := make([]byte, 5)
			copy(cpy, input[pos:pos+4])
			for i := 0; i < (8 - int(shift)); i++ {
				cpy = bitstreamShift(cpy)
			}
			fmt.Printf("OK: %v in %v -> %02x\n", pos, shift, cpy[1:4])
		*/
		pos += 4
		var nv uint32
		switch il - pos {
		case 0, 1:
			break
		case 2:
			tmp := []byte{input[pos], input[pos+1], 0x0, 0x0}
			nv = binary.LittleEndian.Uint32(tmp)
			pos += 2
		case 3:
			tmp := []byte{input[pos], input[pos+1], input[pos+2], 0x0}
			nv = binary.LittleEndian.Uint32(tmp)
			pos += 3
		default:
			nv = binary.LittleEndian.Uint32(input[pos : pos+4])
			pos += 4
		}
		s, ok := second[nv]
		if !ok {
			pos = rpos + 1
			continue
		}
		if s != shift {
			log.Fatalf("internal consistency error: table indicates offset of %v, but found %v", s, shift)
		}
		return rpos, int(shift)
	}
	return -1, -1
}

// findTrailingMagic finds the magic number at the end of the bit stream
// by working backwards to allow for up to 7 bits of trailing padding. It
// returns the CRC that follows that trailer as 4 bytes, the number of bytes
// in the trailer that contain only data from the trailer, and the bit offset
// of the trailer.
func findTrailingMagicAndCRC(buf []byte, trailer []byte) (crc []byte, length int, offsetInBits int) {
	crc = make([]byte, 4)
	aligned := buf[len(buf)-10:]
	if idx := bytes.Index(aligned, trailer); idx == 0 {
		copy(crc, aligned[6:10])
		// 10 is 6 bits of magic and 4 of crc.
		return crc, 10, 0
	}
	unaligned := make([]byte, 11)
	copy(unaligned, buf[len(buf)-11:])
	for p := 0; p < 7; p++ {
		// shift until all of the padding has been consumed
		unaligned = bitstreamShift(unaligned)
		if idx := bytes.Index(unaligned[1:], trailer); idx == 0 {
			copy(crc, unaligned[7:11])
			return crc, 10, (7 - p)
		}
	}
	return nil, -1, -1
}
