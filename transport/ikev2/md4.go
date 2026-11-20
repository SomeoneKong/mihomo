package ikev2

import (
	"encoding/binary"
	"math/bits"
)

// md4Sum implements MD4 (RFC 1320), which MS-CHAPv2 uses for password hashes.
func md4Sum(data []byte) [16]byte {
	msg := append([]byte(nil), data...)
	msg = append(msg, 0x80)
	for len(msg)%64 != 56 {
		msg = append(msg, 0)
	}
	msg = binary.LittleEndian.AppendUint64(msg, uint64(len(data))*8)

	a, b, c, d := uint32(0x67452301), uint32(0xefcdab89), uint32(0x98badcfe), uint32(0x10325476)
	var x [16]uint32
	for block := msg; len(block) > 0; block = block[64:] {
		for i := range x {
			x[i] = binary.LittleEndian.Uint32(block[4*i:])
		}
		aa, bb, cc, dd := a, b, c, d

		f := func(x, y, z uint32) uint32 { return x&y | ^x&z }
		g := func(x, y, z uint32) uint32 { return x&y | x&z | y&z }
		h := func(x, y, z uint32) uint32 { return x ^ y ^ z }

		for _, i := range [4]int{0, 4, 8, 12} {
			a = bits.RotateLeft32(a+f(b, c, d)+x[i], 3)
			d = bits.RotateLeft32(d+f(a, b, c)+x[i+1], 7)
			c = bits.RotateLeft32(c+f(d, a, b)+x[i+2], 11)
			b = bits.RotateLeft32(b+f(c, d, a)+x[i+3], 19)
		}
		for _, i := range [4]int{0, 1, 2, 3} {
			a = bits.RotateLeft32(a+g(b, c, d)+x[i]+0x5a827999, 3)
			d = bits.RotateLeft32(d+g(a, b, c)+x[i+4]+0x5a827999, 5)
			c = bits.RotateLeft32(c+g(d, a, b)+x[i+8]+0x5a827999, 9)
			b = bits.RotateLeft32(b+g(c, d, a)+x[i+12]+0x5a827999, 13)
		}
		for _, i := range [4]int{0, 2, 1, 3} {
			a = bits.RotateLeft32(a+h(b, c, d)+x[i]+0x6ed9eba1, 3)
			d = bits.RotateLeft32(d+h(a, b, c)+x[i+8]+0x6ed9eba1, 9)
			c = bits.RotateLeft32(c+h(d, a, b)+x[i+4]+0x6ed9eba1, 11)
			b = bits.RotateLeft32(b+h(c, d, a)+x[i+12]+0x6ed9eba1, 15)
		}

		a, b, c, d = a+aa, b+bb, c+cc, d+dd
	}
	var out [16]byte
	binary.LittleEndian.PutUint32(out[0:], a)
	binary.LittleEndian.PutUint32(out[4:], b)
	binary.LittleEndian.PutUint32(out[8:], c)
	binary.LittleEndian.PutUint32(out[12:], d)
	return out
}
