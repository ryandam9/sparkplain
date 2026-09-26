package eventlog

import (
	"encoding/binary"
	"math/bits"
)

// xxh32 is the 32-bit xxHash, used by lz4-java to checksum each block.
func xxh32(b []byte, seed uint32) uint32 {
	const (
		p1 = 2654435761
		p2 = 2246822519
		p3 = 3266489917
		p4 = 668265263
		p5 = 374761393
	)
	n := len(b)
	var h uint32
	if n >= 16 {
		v1 := seed + p1 + p2
		v2 := seed + p2
		v3 := seed
		v4 := seed - p1
		for len(b) >= 16 {
			v1 = bits.RotateLeft32(v1+binary.LittleEndian.Uint32(b[0:])*p2, 13) * p1
			v2 = bits.RotateLeft32(v2+binary.LittleEndian.Uint32(b[4:])*p2, 13) * p1
			v3 = bits.RotateLeft32(v3+binary.LittleEndian.Uint32(b[8:])*p2, 13) * p1
			v4 = bits.RotateLeft32(v4+binary.LittleEndian.Uint32(b[12:])*p2, 13) * p1
			b = b[16:]
		}
		h = bits.RotateLeft32(v1, 1) + bits.RotateLeft32(v2, 7) + bits.RotateLeft32(v3, 12) + bits.RotateLeft32(v4, 18)
	} else {
		h = seed + p5
	}
	h += uint32(n)
	for len(b) >= 4 {
		h = bits.RotateLeft32(h+binary.LittleEndian.Uint32(b)*p3, 17) * p4
		b = b[4:]
	}
	for _, c := range b {
		h = bits.RotateLeft32(h+uint32(c)*p5, 11) * p1
	}
	h ^= h >> 15
	h *= p2
	h ^= h >> 13
	h *= p3
	h ^= h >> 16
	return h
}
