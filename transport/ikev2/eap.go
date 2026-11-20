package ikev2

import (
	"crypto/des"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"unicode/utf16"
)

// EAP codes and types (RFC 3748) and EAP-MSCHAPv2 opcodes (draft-kamath-pppext-eap-mschapv2).
const (
	eapCodeRequest  = 1
	eapCodeResponse = 2
	eapCodeSuccess  = 3
	eapCodeFailure  = 4

	eapTypeIdentity = 1
	eapTypeNak      = 3
	eapTypeMSCHAPv2 = 26

	mschapChallenge = 1
	mschapResponse  = 2
	mschapSuccess   = 3
	mschapFailure   = 4
)

// eapPeer is the client side of EAP-Identity followed by EAP-MSCHAPv2.
type eapPeer struct {
	username, password string
	ntResponse         []byte
	authResponse       string
	msk                []byte
}

func eapPacket(code, id uint8, typ uint8, data []byte) []byte {
	b := []byte{code, id, 0, 0, typ}
	b = append(b, data...)
	binary.BigEndian.PutUint16(b[2:], uint16(len(b)))
	return b
}

// respond answers an EAP request.
func (p *eapPeer) respond(req []byte) ([]byte, error) {
	if len(req) < 5 || int(binary.BigEndian.Uint16(req[2:])) != len(req) {
		return nil, errors.New("ikev2: malformed EAP request")
	}
	id, typ, data := req[1], req[4], req[5:]
	switch typ {
	case eapTypeIdentity:
		return eapPacket(eapCodeResponse, id, eapTypeIdentity, []byte(p.username)), nil
	case eapTypeMSCHAPv2:
		out, err := p.mschapv2(data)
		if err != nil {
			return nil, err
		}
		return eapPacket(eapCodeResponse, id, eapTypeMSCHAPv2, out), nil
	default:
		// Legacy Nak proposing EAP-MSCHAPv2.
		return eapPacket(eapCodeResponse, id, eapTypeNak, []byte{eapTypeMSCHAPv2}), nil
	}
}

func (p *eapPeer) mschapv2(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, errors.New("ikev2: malformed EAP-MSCHAPv2 request")
	}
	op, msID := data[0], data[1]
	switch op {
	case mschapChallenge:
		if len(data) < 5 || data[4] != 16 || len(data) < 21 {
			return nil, errors.New("ikev2: malformed EAP-MSCHAPv2 challenge")
		}
		authChallenge := data[5:21]
		peerChallenge := randomBytes(16)
		p.ntResponse = generateNTResponse(authChallenge, peerChallenge, p.username, p.password)
		p.authResponse = authenticatorResponse(p.password, p.ntResponse, peerChallenge, authChallenge, p.username)
		value := concat(peerChallenge, make([]byte, 8), p.ntResponse, []byte{0})
		out := []byte{mschapResponse, msID, 0, 0, byte(len(value))}
		out = append(out, value...)
		out = append(out, p.username...)
		binary.BigEndian.PutUint16(out[2:], uint16(len(out)))
		return out, nil
	case mschapSuccess:
		if p.ntResponse == nil {
			return nil, errors.New("ikev2: EAP-MSCHAPv2 success before challenge")
		}
		message := string(data[4:])
		if len(message) < 42 || subtle.ConstantTimeCompare([]byte(strings.ToUpper(message[:42])), []byte(p.authResponse)) != 1 {
			return nil, errors.New("ikev2: EAP-MSCHAPv2 server authenticator response mismatch")
		}
		p.msk = mschapv2MSK(p.password, p.ntResponse)
		return []byte{mschapSuccess}, nil
	case mschapFailure:
		return nil, fmt.Errorf("ikev2: EAP-MSCHAPv2 authentication failed: %s", strings.TrimSpace(string(data[4:])))
	default:
		return nil, fmt.Errorf("ikev2: unexpected EAP-MSCHAPv2 opcode %d", op)
	}
}

// RFC 2759 section 8.

func ntPasswordHash(password string) []byte {
	units := utf16.Encode([]rune(password))
	b := make([]byte, 2*len(units))
	for i, u := range units {
		binary.LittleEndian.PutUint16(b[2*i:], u)
	}
	h := md4Sum(b)
	return h[:]
}

func challengeHash(peerChallenge, authChallenge []byte, username string) []byte {
	if i := strings.LastIndexByte(username, '\\'); i >= 0 {
		username = username[i+1:]
	}
	h := sha1.Sum(concat(peerChallenge, authChallenge, []byte(username)))
	return h[:8]
}

func challengeResponse(challenge, passwordHash []byte) []byte {
	z := append(append([]byte(nil), passwordHash...), make([]byte, 5)...)
	out := make([]byte, 0, 24)
	for i := 0; i < 3; i++ {
		block, _ := des.NewCipher(desKey(z[7*i : 7*i+7]))
		c := make([]byte, 8)
		block.Encrypt(c, challenge)
		out = append(out, c...)
	}
	return out
}

// desKey spreads 56 key bits over 8 bytes; DES ignores the parity bits.
func desKey(k []byte) []byte {
	return []byte{
		k[0] &^ 1,
		k[0]<<7 | k[1]>>1,
		k[1]<<6 | k[2]>>2,
		k[2]<<5 | k[3]>>3,
		k[3]<<4 | k[4]>>4,
		k[4]<<3 | k[5]>>5,
		k[5]<<2 | k[6]>>6,
		k[6] << 1,
	}
}

func generateNTResponse(authChallenge, peerChallenge []byte, username, password string) []byte {
	return challengeResponse(challengeHash(peerChallenge, authChallenge, username), ntPasswordHash(password))
}

var (
	authMagic1 = []byte("Magic server to client signing constant")
	authMagic2 = []byte("Pad to make it do more than one iteration")
)

func authenticatorResponse(password string, ntResponse, peerChallenge, authChallenge []byte, username string) string {
	hashHash := md4Sum(ntPasswordHash(password))
	digest := sha1.Sum(concat(hashHash[:], ntResponse, authMagic1))
	digest = sha1.Sum(concat(digest[:], challengeHash(peerChallenge, authChallenge, username), authMagic2))
	return "S=" + strings.ToUpper(hex.EncodeToString(digest[:]))
}

// RFC 3079 section 3.4.

var (
	mppeMagic1 = []byte("This is the MPPE Master Key")
	mppeMagic2 = []byte("On the client side, this is the send key; on the server side, it is the receive key.")
	mppeMagic3 = []byte("On the client side, this is the receive key; on the server side, it is the send key.")
	shsPad1    = make([]byte, 40)
	shsPad2    = []byte(strings.Repeat("\xf2", 40))
)

func mppeMasterKey(passwordHashHash, ntResponse []byte) []byte {
	d := sha1.Sum(concat(passwordHashHash, ntResponse, mppeMagic1))
	return d[:16]
}

func mppeStartKey(masterKey, magic []byte) []byte {
	d := sha1.Sum(concat(masterKey, shsPad1, magic, shsPad2))
	return d[:16]
}

// mschapv2MSK is the EAP-MSCHAPv2 MSK: the client send key followed by the client receive key, as the
// MS-MPPE-Recv-Key and MS-MPPE-Send-Key of the server. Trailing zero padding to 64 bytes would not change
// the IKEv2 AUTH value, since HMAC pads short keys with zeros.
func mschapv2MSK(password string, ntResponse []byte) []byte {
	hashHash := md4Sum(ntPasswordHash(password))
	master := mppeMasterKey(hashHash[:], ntResponse)
	return concat(mppeStartKey(master, mppeMagic2), mppeStartKey(master, mppeMagic3))
}
