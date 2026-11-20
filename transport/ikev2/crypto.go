package ikev2

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math/big"
)

// Transform types and IDs from the IANA IKEv2 registry (RFC 7296 section 3.3.2).
const (
	transformENCR  = 1
	transformPRF   = 2
	transformINTEG = 3
	transformDH    = 4
	transformESN   = 5

	encrAESCBC    = 12
	attrKeyLength = 14
)

// hashAlg is a hash used both as an HMAC integrity transform and as an HMAC PRF.
type hashAlg struct {
	name    string
	new     func() hash.Hash
	size    int
	prfID   uint16
	integID uint16
	icvLen  int
}

var hashAlgs = []*hashAlg{
	{name: "sha1", new: sha1.New, size: 20, prfID: 2, integID: 2, icvLen: 12},
	{name: "sha256", new: sha256.New, size: 32, prfID: 5, integID: 12, icvLen: 16},
	{name: "sha384", new: sha512.New384, size: 48, prfID: 6, integID: 13, icvLen: 24},
	{name: "sha512", new: sha512.New, size: 64, prfID: 7, integID: 14, icvLen: 32},
}

func (h *hashAlg) prf(key []byte, data ...[]byte) []byte {
	m := hmac.New(h.new, key)
	for _, d := range data {
		m.Write(d)
	}
	return m.Sum(nil)
}

// prfPlus is prf+ from RFC 7296 section 2.13.
func (h *hashAlg) prfPlus(key, seed []byte, length int) []byte {
	out := make([]byte, 0, length+h.size)
	var t []byte
	for i := byte(1); len(out) < length; i++ {
		t = h.prf(key, t, seed, []byte{i})
		out = append(out, t...)
	}
	return out[:length]
}

func (h *hashAlg) icv(key []byte, data []byte) []byte {
	return h.prf(key, data)[:h.icvLen]
}

func newAESCBC(key []byte) (cipher.Block, error) {
	return aes.NewCipher(key)
}

// dhGroup is a Diffie-Hellman group: an ECP group from RFC 5903 or a MODP group from RFC 3526.
type dhGroup struct {
	id    uint16
	name  string
	curve ecdh.Curve
	p     *big.Int
}

var dhGroups = []*dhGroup{
	{id: 14, name: "modp2048", p: mustPrime(modp2048)},
	{id: 15, name: "modp3072", p: mustPrime(modp3072)},
	{id: 19, name: "ecp256", curve: ecdh.P256()},
	{id: 20, name: "ecp384", curve: ecdh.P384()},
	{id: 21, name: "ecp521", curve: ecdh.P521()},
}

type dhPrivate struct {
	group *dhGroup
	key   *ecdh.PrivateKey
	x     *big.Int
	pub   []byte
}

func (g *dhGroup) generate() (*dhPrivate, error) {
	if g.curve != nil {
		key, err := g.curve.GenerateKey(rand.Reader)
		if err != nil {
			return nil, err
		}
		// RFC 5903 encodes the public value as x || y without the SEC1 0x04 prefix.
		return &dhPrivate{group: g, key: key, pub: key.PublicKey().Bytes()[1:]}, nil
	}
	x, err := rand.Int(rand.Reader, new(big.Int).Sub(g.p, big.NewInt(3)))
	if err != nil {
		return nil, err
	}
	x.Add(x, big.NewInt(2))
	pub := new(big.Int).Exp(big.NewInt(2), x, g.p)
	return &dhPrivate{group: g, x: x, pub: pub.FillBytes(make([]byte, (g.p.BitLen()+7)/8))}, nil
}

func (k *dhPrivate) shared(peer []byte) ([]byte, error) {
	g := k.group
	if g.curve != nil {
		pub, err := g.curve.NewPublicKey(append([]byte{4}, peer...))
		if err != nil {
			return nil, fmt.Errorf("invalid %s public value: %w", g.name, err)
		}
		// The shared secret of an ECP group is the x coordinate (RFC 5903 section 7).
		return k.key.ECDH(pub)
	}
	size := (g.p.BitLen() + 7) / 8
	y := new(big.Int).SetBytes(peer)
	if len(peer) != size || y.Cmp(big.NewInt(1)) <= 0 || y.Cmp(new(big.Int).Sub(g.p, big.NewInt(1))) >= 0 {
		return nil, fmt.Errorf("invalid %s public value", g.name)
	}
	return new(big.Int).Exp(y, k.x, g.p).FillBytes(make([]byte, size)), nil
}

func mustPrime(hexValue string) *big.Int {
	p, ok := new(big.Int).SetString(hexValue, 16)
	if !ok {
		panic("ikev2: invalid MODP prime")
	}
	return p
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

func randomSPI32() uint32 {
	for {
		if v := binary.BigEndian.Uint32(randomBytes(4)); v >= 256 {
			// SPIs 1-255 are reserved by IANA (RFC 4303 section 2.1).
			return v
		}
	}
}

func randomSPI64() uint64 {
	for {
		if v := binary.BigEndian.Uint64(randomBytes(8)); v != 0 {
			return v
		}
	}
}

var errShortPacket = errors.New("ikev2: packet too short")

// RFC 3526 MODP primes.
const (
	modp2048 = "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7EDEE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3DC2007CB8A163BF0598DA48361C55D39A69163FA8FD24CF5F83655D23DCA3AD961C62F356208552BB9ED529077096966D670C354E4ABC9804F1746C08CA18217C32905E462E36CE3BE39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9DE2BCBF6955817183995497CEA956AE515D2261898FA051015728E5A8AACAA68FFFFFFFFFFFFFFFF"
	modp3072 = "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7EDEE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3DC2007CB8A163BF0598DA48361C55D39A69163FA8FD24CF5F83655D23DCA3AD961C62F356208552BB9ED529077096966D670C354E4ABC9804F1746C08CA18217C32905E462E36CE3BE39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9DE2BCBF6955817183995497CEA956AE515D2261898FA051015728E5A8AAAC42DAD33170D04507A33A85521ABDF1CBA64ECFB850458DBEF0A8AEA71575D060C7DB3970F85A6E1E4C7ABF5AE8CDB0933D71E8C94E04A25619DCEE3D2261AD2EE6BF12FFA06D98A0864D87602733EC86A64521F2B18177B200CBBE117577A615D6C770988C0BAD946E208E24FA074E5AB3143DB5BFCE0FD108E4B82D120A93AD2CAFFFFFFFFFFFFFFFF"
)
