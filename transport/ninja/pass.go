package ninja

import (
	"crypto/cipher"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"

	"golang.org/x/crypto/hkdf"
	"golang.org/x/crypto/sha3"
)

var passSalt, _ = hex.DecodeString("d8b8c066c10e8b0d1b1ba3ad1f7de1525625d03f9d9f682f1bd6097b13a22b38")

func passHKDF(key, salt, info []byte, n int) []byte {
	out := make([]byte, n)
	if _, err := io.ReadFull(hkdf.New(sha3.New512, key, salt, info), out); err != nil {
		panic(err)
	}
	return out
}

type passState struct {
	a     cipher.AEAD
	nonce []byte
	shake sha3.ShakeHash
}
type passConn struct {
	net.Conn
	master, verify        []byte
	readState, writeState *passState
	first                 bool
	padMin, padMax        int
	pending               []byte
}

func newPass(c net.Conn, n Options) *passConn {
	h := sha3.Sum512([]byte(n.PassOpts.Password))
	master := append([]byte(nil), h[:16]...)
	return &passConn{Conn: c, master: master, verify: passHKDF(master, passSalt, []byte(n.PassOpts.Password), 16), padMin: n.PassOpts.PaddingMin, padMax: n.PassOpts.PaddingMax}
}
func (c *passConn) state(iv []byte) *passState {
	key := passHKDF(c.master, iv, passSalt, 16)
	// The clear IV is also the initial nonce; SHAKE masks the two frame lengths.
	shake := sha3.NewShake128()
	shake.Write(iv)
	shake.Write(key)
	return &passState{a: aead(key), nonce: append([]byte(nil), iv...), shake: shake}
}
func (s *passState) mask() uint16 {
	var b [2]byte
	io.ReadFull(s.shake, b[:])
	return binary.BigEndian.Uint16(b[:])
}
func (s *passState) seal(b []byte) []byte {
	v := s.a.Seal(nil, s.nonce, b, nil)
	increment(s.nonce)
	return v
}
func (s *passState) open(b []byte) ([]byte, error) {
	v, e := s.a.Open(nil, s.nonce, b, nil)
	if e == nil {
		increment(s.nonce)
	}
	return v, e
}
func (c *passConn) Write(b []byte) (int, error) {
	n := len(b)
	if !c.first {
		// This verification prefix is inside the first encrypted pass frame.
		b = append(append(append([]byte(nil), c.verify...), 1), b...)
		c.first = true
	}
	var packet []byte
	if c.writeState == nil {
		iv := random(12)
		c.writeState = c.state(iv)
		packet = append(packet, iv...)
	}
	for len(b) > 0 {
		count := minInt(len(b), 65535)
		pad := c.padMin + randomN(c.padMax)
		header := make([]byte, 4)
		binary.BigEndian.PutUint16(header, uint16(count)^c.writeState.mask())
		binary.BigEndian.PutUint16(header[2:], uint16(pad)^c.writeState.mask())
		packet = append(packet, c.writeState.seal(header)...)
		packet = append(packet, c.writeState.seal(append(append([]byte(nil), b[:count]...), random(pad)...))...)
		b = b[count:]
	}
	if err := writeAll(c.Conn, packet); err != nil {
		return 0, err
	}
	return n, nil
}
func (c *passConn) Read(b []byte) (int, error) {
	for len(c.pending) == 0 {
		readHead := readN
		if c.readState == nil {
			iv, e := readN(c.Conn, 12)
			if e != nil {
				return 0, readError("pass IV", e)
			}
			c.readState = c.state(iv)
			readHead = readRest
		}
		head, e := readHead(c.Conn, 20)
		if e != nil {
			return 0, readError("pass header read", e)
		}
		head, e = c.readState.open(head)
		if e != nil {
			return 0, fmt.Errorf("pass header AEAD: %w", e)
		}
		size := int(binary.BigEndian.Uint16(head) ^ c.readState.mask())
		pad := int(binary.BigEndian.Uint16(head[2:]) ^ c.readState.mask())
		data, e := readRest(c.Conn, size+pad+16)
		if e != nil {
			return 0, e
		}
		data, e = c.readState.open(data)
		if e != nil {
			return 0, fmt.Errorf("pass payload AEAD: %w", e)
		}
		c.pending = data[:size]
	}
	n := copy(b, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}
