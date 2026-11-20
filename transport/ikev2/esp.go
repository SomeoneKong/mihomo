package ikev2

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"encoding/binary"
	"errors"
	"hash"
	"sync"
	"sync/atomic"
)

const (
	nextHeaderIPv4  = 4
	nextHeaderIPv6  = 41
	nextHeaderNoNxt = 59

	replayWindow = 64
	// Rekey well before the 32-bit sequence number wraps, as extended sequence numbers are not used.
	seqRekeyThreshold = 0xf0000000
)

var errReplay = errors.New("ikev2: replayed ESP packet")

// espSA is one direction of a CHILD SA using AES-CBC with an HMAC integrity check (RFC 4303),
// carried in UDP (RFC 3948).
type espSA struct {
	spi   uint32
	block cipher.Block
	integ *hashAlg
	macs  sync.Pool

	seq atomic.Uint32 // outbound

	mu        sync.Mutex // inbound replay window
	replayTop uint32
	replayMap uint64
}

func newESPSA(spi uint32, s *suite, encKey, integKey []byte) (*espSA, error) {
	block, err := aes.NewCipher(encKey)
	if err != nil {
		return nil, err
	}
	sa := &espSA{spi: spi, block: block, integ: s.integ}
	sa.macs.New = func() any { return hmac.New(s.integ.new, integKey) }
	return sa, nil
}

func (sa *espSA) icv(data []byte) []byte {
	m := sa.macs.Get().(hash.Hash)
	m.Reset()
	m.Write(data)
	sum := m.Sum(nil)
	sa.macs.Put(m)
	return sum[:sa.integ.icvLen]
}

// seal encapsulates an IP packet.
func (sa *espSA) seal(packet []byte) ([]byte, error) {
	if len(packet) == 0 {
		return nil, errors.New("ikev2: empty packet")
	}
	nextHeader := byte(nextHeaderIPv4)
	if packet[0]>>4 == 6 {
		nextHeader = nextHeaderIPv6
	}
	padLen := (aes.BlockSize - (len(packet)+2)%aes.BlockSize) % aes.BlockSize
	plainLen := len(packet) + padLen + 2
	out := make([]byte, 8+aes.BlockSize+plainLen, 8+aes.BlockSize+plainLen+sa.integ.icvLen)
	binary.BigEndian.PutUint32(out[0:], sa.spi)
	binary.BigEndian.PutUint32(out[4:], sa.seq.Add(1))
	iv := out[8 : 8+aes.BlockSize]
	copy(iv, randomBytes(aes.BlockSize))
	plain := out[8+aes.BlockSize:]
	n := copy(plain, packet)
	for i := 1; i <= padLen; i++ {
		plain[n] = byte(i)
		n++
	}
	plain[n] = byte(padLen)
	plain[n+1] = nextHeader
	cipher.NewCBCEncrypter(sa.block, iv).CryptBlocks(plain, plain)
	return append(out, sa.icv(out)...), nil
}

// open verifies and decapsulates an ESP packet. A nil packet with a nil error is a dummy packet.
func (sa *espSA) open(b []byte) ([]byte, error) {
	icvLen := sa.integ.icvLen
	if len(b) < 8+2*aes.BlockSize+icvLen || (len(b)-8-icvLen)%aes.BlockSize != 0 {
		return nil, errShortPacket
	}
	body, sum := b[:len(b)-icvLen], b[len(b)-icvLen:]
	if !hmac.Equal(sa.icv(body), sum) {
		return nil, errors.New("ikev2: ESP integrity check failed")
	}
	if !sa.checkReplay(binary.BigEndian.Uint32(b[4:])) {
		return nil, errReplay
	}
	iv, ct := body[8:8+aes.BlockSize], body[8+aes.BlockSize:]
	plain := make([]byte, len(ct))
	cipher.NewCBCDecrypter(sa.block, iv).CryptBlocks(plain, ct)
	padLen, nextHeader := int(plain[len(plain)-2]), plain[len(plain)-1]
	if padLen+2 > len(plain) {
		return nil, errors.New("ikev2: bad ESP padding")
	}
	if nextHeader == nextHeaderNoNxt {
		return nil, nil
	}
	return plain[:len(plain)-padLen-2], nil
}

// checkReplay implements the sliding window of RFC 4303 section 3.4.3 and records seq when it is new.
func (sa *espSA) checkReplay(seq uint32) bool {
	if seq == 0 {
		return false
	}
	sa.mu.Lock()
	defer sa.mu.Unlock()
	if seq > sa.replayTop {
		shift := seq - sa.replayTop
		if shift >= replayWindow {
			sa.replayMap = 1
		} else {
			sa.replayMap = sa.replayMap<<shift | 1
		}
		sa.replayTop = seq
		return true
	}
	diff := sa.replayTop - seq
	if diff >= replayWindow || sa.replayMap&(1<<diff) != 0 {
		return false
	}
	sa.replayMap |= 1 << diff
	return true
}

// childSA is a pair of ESP SAs and the configured proposal it was negotiated from.
type childSA struct {
	in, out *espSA
	suite   *suite
}

func newChildSA(prf *hashAlg, skD []byte, s *suite, inSPI, outSPI uint32, seed []byte) (*childSA, error) {
	encLen, integLen := s.encrKeyLen, s.integ.size
	km := prf.prfPlus(skD, seed, 2*(encLen+integLen))
	// Keys for the initiator to responder direction come first (RFC 7296 section 2.17).
	out, err := newESPSA(outSPI, s, km[:encLen], km[encLen:encLen+integLen])
	if err != nil {
		return nil, err
	}
	km = km[encLen+integLen:]
	in, err := newESPSA(inSPI, s, km[:encLen], km[encLen:])
	if err != nil {
		return nil, err
	}
	return &childSA{in: in, out: out, suite: s}, nil
}
