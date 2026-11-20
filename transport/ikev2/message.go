package ikev2

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
)

// Exchange types, flags, payload types, notify types and attribute types from RFC 7296.
const (
	exchangeIKESAInit     = 34
	exchangeIKEAuth       = 35
	exchangeCreateChildSA = 36
	exchangeInformational = 37

	flagInitiator = 0x08
	flagResponse  = 0x20

	payloadSA    = 33
	payloadKE    = 34
	payloadIDi   = 35
	payloadIDr   = 36
	payloadAUTH  = 39
	payloadNonce = 40
	payloadN     = 41
	payloadD     = 42
	payloadTSi   = 44
	payloadTSr   = 45
	payloadSK    = 46
	payloadCP    = 47
	payloadEAP   = 48

	protoIKE = 1
	protoESP = 3

	notifyInvalidKEPayload     = 17
	notifyNoAdditionalSAs      = 35
	notifyTemporaryFailure     = 43
	notifyCookie               = 16390
	notifyNATDetectionSourceIP = 16388
	notifyNATDetectionDestIP   = 16389
	notifyRekeySA              = 16393
	notifyErrorMax             = 16383

	idIPv4  = 1
	idFQDN  = 2
	idEmail = 3
	idIPv6  = 5
	idKeyID = 11

	authSharedKey = 2

	cfgRequest = 1
	cfgReply   = 2

	attrInternalIP4Address = 1
	attrInternalIP4DNS     = 3
	attrInternalIP6Address = 8
	attrInternalIP6DNS     = 10

	tsIPv4Range = 7
	tsIPv6Range = 8

	headerLen = 28
)

var notifyErrorNames = map[uint16]string{
	1: "UNSUPPORTED_CRITICAL_PAYLOAD", 4: "INVALID_IKE_SPI", 5: "INVALID_MAJOR_VERSION", 7: "INVALID_SYNTAX",
	9: "INVALID_MESSAGE_ID", 11: "INVALID_SPI", 14: "NO_PROPOSAL_CHOSEN", 17: "INVALID_KE_PAYLOAD",
	24: "AUTHENTICATION_FAILED", 34: "SINGLE_PAIR_REQUIRED", 35: "NO_ADDITIONAL_SAS", 36: "INTERNAL_ADDRESS_FAILURE",
	37: "FAILED_CP_REQUIRED", 38: "TS_UNACCEPTABLE", 39: "INVALID_SELECTORS", 43: "TEMPORARY_FAILURE",
	44: "CHILD_SA_NOT_FOUND",
}

type payload struct {
	typ  uint8
	body []byte
}

type message struct {
	spiI, spiR uint64
	exchange   uint8
	flags      uint8
	msgID      uint32
	payloads   []payload
}

func (m *message) isResponse() bool { return m.flags&flagResponse != 0 }

func (m *message) find(typ uint8) []byte {
	for _, p := range m.payloads {
		if p.typ == typ {
			return p.body
		}
	}
	return nil
}

func (m *message) notifies() []notify {
	var out []notify
	for _, p := range m.payloads {
		if p.typ == payloadN {
			if n, err := parseNotify(p.body); err == nil {
				out = append(out, n)
			}
		}
	}
	return out
}

func (m *message) notify(typ uint16) *notify {
	for _, n := range m.notifies() {
		if n.typ == typ {
			return &n
		}
	}
	return nil
}

// errorNotify returns the first error notification of the message, if any.
func (m *message) errorNotify() error {
	for _, n := range m.notifies() {
		if n.typ <= notifyErrorMax {
			name := notifyErrorNames[n.typ]
			if name == "" {
				name = fmt.Sprintf("error %d", n.typ)
			}
			return &NotifyError{Type: n.typ, Name: name}
		}
	}
	return nil
}

// NotifyError is an error notification received from the peer.
type NotifyError struct {
	Type uint16
	Name string
}

func (e *NotifyError) Error() string { return "ikev2: peer replied " + e.Name }

func encodeChain(payloads []payload) (uint8, []byte) {
	var out []byte
	for i, p := range payloads {
		next := uint8(0)
		if i+1 < len(payloads) {
			next = payloads[i+1].typ
		}
		out = append(out, next, 0, 0, 0)
		binary.BigEndian.PutUint16(out[len(out)-2:], uint16(4+len(p.body)))
		out = append(out, p.body...)
	}
	if len(payloads) == 0 {
		return 0, out
	}
	return payloads[0].typ, out
}

func (m *message) header(next uint8, length int) []byte {
	b := make([]byte, headerLen, length)
	binary.BigEndian.PutUint64(b[0:], m.spiI)
	binary.BigEndian.PutUint64(b[8:], m.spiR)
	b[16] = next
	b[17] = 0x20
	b[18] = m.exchange
	b[19] = m.flags
	binary.BigEndian.PutUint32(b[20:], m.msgID)
	binary.BigEndian.PutUint32(b[24:], uint32(length))
	return b
}

func (m *message) encodePlain() []byte {
	first, chain := encodeChain(m.payloads)
	return append(m.header(first, headerLen+len(chain)), chain...)
}

// parseMessage parses the header and the unencrypted payloads. An SK payload, which must be the last one,
// is returned undecrypted together with the type of its first inner payload.
func parseMessage(b []byte) (m *message, skNext uint8, sk []byte, err error) {
	if len(b) < headerLen {
		return nil, 0, nil, errShortPacket
	}
	if b[17]>>4 != 2 {
		return nil, 0, nil, errors.New("ikev2: unsupported IKE version")
	}
	if int(binary.BigEndian.Uint32(b[24:])) != len(b) {
		return nil, 0, nil, errors.New("ikev2: bad message length")
	}
	m = &message{
		spiI:     binary.BigEndian.Uint64(b[0:]),
		spiR:     binary.BigEndian.Uint64(b[8:]),
		exchange: b[18],
		flags:    b[19],
		msgID:    binary.BigEndian.Uint32(b[20:]),
	}
	next := b[16]
	rest := b[headerLen:]
	for next != 0 {
		if len(rest) < 4 {
			return nil, 0, nil, errShortPacket
		}
		length := int(binary.BigEndian.Uint16(rest[2:]))
		if length < 4 || length > len(rest) {
			return nil, 0, nil, errors.New("ikev2: bad payload length")
		}
		if next == payloadSK {
			return m, rest[0], rest[4:length], nil
		}
		m.payloads = append(m.payloads, payload{typ: next, body: rest[4:length]})
		next = rest[0]
		rest = rest[length:]
	}
	return m, 0, nil, nil
}

func parseChain(next uint8, b []byte) ([]payload, error) {
	var out []payload
	for next != 0 {
		if len(b) < 4 {
			return nil, errShortPacket
		}
		length := int(binary.BigEndian.Uint16(b[2:]))
		if length < 4 || length > len(b) {
			return nil, errors.New("ikev2: bad payload length")
		}
		out = append(out, payload{typ: next, body: b[4:length]})
		next = b[0]
		b = b[length:]
	}
	return out, nil
}

// ikeKeys holds the keys of an IKE SA. The client is always the original initiator of its IKE SAs,
// so it encrypts with SK_ei/SK_ai and decrypts with SK_er/SK_ar.
type ikeKeys struct {
	suite                       *suite
	skD, skAI, skAR, skPI, skPR []byte
	encI, encR                  cipher.Block
}

func deriveIKEKeys(s *suite, skeyseed, ni, nr []byte, spiI, spiR uint64) (*ikeKeys, error) {
	spis := make([]byte, 16)
	binary.BigEndian.PutUint64(spis[0:], spiI)
	binary.BigEndian.PutUint64(spis[8:], spiR)
	prfLen, integLen, encLen := s.integ.size, s.integ.size, s.encrKeyLen
	km := s.integ.prfPlus(skeyseed, concat(ni, nr, spis), 3*prfLen+2*integLen+2*encLen)
	take := func(n int) []byte {
		v := km[:n:n]
		km = km[n:]
		return v
	}
	k := &ikeKeys{suite: s}
	k.skD = take(prfLen)
	k.skAI = take(integLen)
	k.skAR = take(integLen)
	skEI := take(encLen)
	skER := take(encLen)
	k.skPI = take(prfLen)
	k.skPR = take(prfLen)
	var err error
	if k.encI, err = newAESCBC(skEI); err != nil {
		return nil, err
	}
	if k.encR, err = newAESCBC(skER); err != nil {
		return nil, err
	}
	return k, nil
}

// seal encodes m inside an SK payload (RFC 7296 section 3.14) with the initiator keys.
func (k *ikeKeys) seal(m *message) []byte {
	first, inner := encodeChain(m.payloads)
	padLen := (aes.BlockSize - (len(inner)+1)%aes.BlockSize) % aes.BlockSize
	plain := append(inner, make([]byte, padLen+1)...)
	plain[len(plain)-1] = byte(padLen)
	icvLen := k.suite.integ.icvLen
	skLen := 4 + aes.BlockSize + len(plain) + icvLen
	b := m.header(payloadSK, headerLen+skLen)
	b = append(b, first, 0, byte(skLen>>8), byte(skLen))
	iv := randomBytes(aes.BlockSize)
	b = append(b, iv...)
	start := len(b)
	b = append(b, plain...)
	cipher.NewCBCEncrypter(k.encI, iv).CryptBlocks(b[start:], b[start:])
	return append(b, k.suite.integ.icv(k.skAI, b)...)
}

// open verifies and decrypts a message sent by the responder.
func (k *ikeKeys) open(raw []byte) (*message, error) {
	m, skNext, sk, err := parseMessage(raw)
	if err != nil {
		return nil, err
	}
	if sk == nil {
		return nil, errors.New("ikev2: message is not encrypted")
	}
	icvLen := k.suite.integ.icvLen
	if len(sk) < aes.BlockSize*2+icvLen || (len(sk)-icvLen)%aes.BlockSize != 0 {
		return nil, errShortPacket
	}
	if !hmac.Equal(k.suite.integ.icv(k.skAR, raw[:len(raw)-icvLen]), raw[len(raw)-icvLen:]) {
		return nil, errors.New("ikev2: integrity check failed")
	}
	iv, ct := sk[:aes.BlockSize], sk[aes.BlockSize:len(sk)-icvLen]
	plain := make([]byte, len(ct))
	cipher.NewCBCDecrypter(k.encR, iv).CryptBlocks(plain, ct)
	padLen := int(plain[len(plain)-1])
	if padLen+1 > len(plain) {
		return nil, errors.New("ikev2: bad padding")
	}
	inner, err := parseChain(skNext, plain[:len(plain)-padLen-1])
	if err != nil {
		return nil, err
	}
	m.payloads = append(m.payloads, inner...)
	return m, nil
}

type transform struct {
	typ    uint8
	id     uint16
	keyLen uint16
}

type proposal struct {
	num        uint8
	proto      uint8
	spi        []byte
	transforms []transform
}

func encodeSA(props []proposal) []byte {
	var out []byte
	for i, p := range props {
		var ts []byte
		for j, t := range p.transforms {
			last := byte(3)
			if j == len(p.transforms)-1 {
				last = 0
			}
			length := 8
			if t.keyLen != 0 {
				length = 12
			}
			ts = append(ts, last, 0, byte(length>>8), byte(length), t.typ, 0, byte(t.id>>8), byte(t.id))
			if t.keyLen != 0 {
				ts = append(ts, 0x80, attrKeyLength, byte(t.keyLen>>8), byte(t.keyLen))
			}
		}
		last := byte(2)
		if i == len(props)-1 {
			last = 0
		}
		length := 8 + len(p.spi) + len(ts)
		out = append(out, last, 0, byte(length>>8), byte(length), p.num, p.proto, byte(len(p.spi)), byte(len(p.transforms)))
		out = append(out, p.spi...)
		out = append(out, ts...)
	}
	return out
}

func parseSA(b []byte) ([]proposal, error) {
	var out []proposal
	for len(b) > 0 {
		if len(b) < 8 {
			return nil, errShortPacket
		}
		length := int(binary.BigEndian.Uint16(b[2:]))
		spiSize := int(b[6])
		if length < 8+spiSize || length > len(b) {
			return nil, errors.New("ikev2: bad proposal length")
		}
		p := proposal{num: b[4], proto: b[5], spi: b[8 : 8+spiSize]}
		ts := b[8+spiSize : length]
		for n := int(b[7]); n > 0; n-- {
			if len(ts) < 8 {
				return nil, errShortPacket
			}
			tl := int(binary.BigEndian.Uint16(ts[2:]))
			if tl < 8 || tl > len(ts) {
				return nil, errors.New("ikev2: bad transform length")
			}
			t := transform{typ: ts[4], id: binary.BigEndian.Uint16(ts[6:])}
			for attrs := ts[8:tl]; len(attrs) >= 4; {
				at := binary.BigEndian.Uint16(attrs)
				if at&0x8000 != 0 {
					if at&0x7fff == attrKeyLength {
						t.keyLen = binary.BigEndian.Uint16(attrs[2:])
					}
					attrs = attrs[4:]
					continue
				}
				al := 4 + int(binary.BigEndian.Uint16(attrs[2:]))
				if al > len(attrs) {
					break
				}
				attrs = attrs[al:]
			}
			p.transforms = append(p.transforms, t)
			ts = ts[tl:]
		}
		out = append(out, p)
		if b[0] == 0 {
			break
		}
		b = b[length:]
	}
	return out, nil
}

// chosenProposal returns the single proposal a responder selected.
func chosenProposal(m *message) (*proposal, error) {
	body := m.find(payloadSA)
	if body == nil {
		return nil, errors.New("ikev2: response has no SA payload")
	}
	props, err := parseSA(body)
	if err != nil {
		return nil, err
	}
	if len(props) != 1 {
		return nil, errors.New("ikev2: responder must choose exactly one proposal")
	}
	return &props[0], nil
}

func encodeKE(group uint16, data []byte) []byte {
	return append([]byte{byte(group >> 8), byte(group), 0, 0}, data...)
}

func parseKE(b []byte) (uint16, []byte, error) {
	if len(b) < 4 {
		return 0, nil, errShortPacket
	}
	return binary.BigEndian.Uint16(b), b[4:], nil
}

type notify struct {
	proto uint8
	typ   uint16
	spi   []byte
	data  []byte
}

func encodeNotify(proto uint8, typ uint16, spi, data []byte) []byte {
	b := []byte{proto, byte(len(spi)), byte(typ >> 8), byte(typ)}
	return append(append(b, spi...), data...)
}

func parseNotify(b []byte) (notify, error) {
	if len(b) < 4 || len(b) < 4+int(b[1]) {
		return notify{}, errShortPacket
	}
	return notify{proto: b[0], typ: binary.BigEndian.Uint16(b[2:]), spi: b[4 : 4+int(b[1])], data: b[4+int(b[1]):]}, nil
}

func encodeDelete(proto uint8, spis ...uint32) []byte {
	if proto == protoIKE {
		return []byte{protoIKE, 0, 0, 0}
	}
	b := []byte{proto, 4, byte(len(spis) >> 8), byte(len(spis))}
	for _, spi := range spis {
		b = binary.BigEndian.AppendUint32(b, spi)
	}
	return b
}

// parseDelete returns the protocol and, for ESP, the SPIs listed in a Delete payload.
func parseDelete(b []byte) (uint8, []uint32, error) {
	if len(b) < 4 {
		return 0, nil, errShortPacket
	}
	proto, size, n := b[0], int(b[1]), int(binary.BigEndian.Uint16(b[2:]))
	if proto != protoESP {
		return proto, nil, nil
	}
	if size != 4 || len(b) < 4+4*n {
		return 0, nil, errors.New("ikev2: bad delete payload")
	}
	spis := make([]uint32, n)
	for i := range spis {
		spis[i] = binary.BigEndian.Uint32(b[4+4*i:])
	}
	return proto, spis, nil
}

func encodeID(typ uint8, data []byte) []byte {
	return append([]byte{typ, 0, 0, 0}, data...)
}

func encodeAuth(data []byte) []byte {
	return append([]byte{authSharedKey, 0, 0, 0}, data...)
}

// encodeTSAny is a traffic selector covering all IPv4 addresses, ports and protocols.
func encodeTSAny() []byte {
	return []byte{1, 0, 0, 0, tsIPv4Range, 0, 0, 16, 0, 0, 0xff, 0xff, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff}
}

// parseTS converts traffic selectors to the prefixes covering their address ranges.
func parseTS(b []byte) ([]netip.Prefix, error) {
	if len(b) < 4 {
		return nil, errShortPacket
	}
	var out []netip.Prefix
	rest := b[4:]
	for n := int(b[0]); n > 0; n-- {
		if len(rest) < 8 {
			return nil, errShortPacket
		}
		length := int(binary.BigEndian.Uint16(rest[2:]))
		if length < 8 || length > len(rest) {
			return nil, errors.New("ikev2: bad traffic selector length")
		}
		addrLen := (length - 8) / 2
		if (rest[0] == tsIPv4Range && addrLen == 4) || (rest[0] == tsIPv6Range && addrLen == 16) {
			start, _ := netip.AddrFromSlice(rest[8 : 8+addrLen])
			end, _ := netip.AddrFromSlice(rest[8+addrLen : length])
			out = append(out, rangeToPrefixes(start, end)...)
		}
		rest = rest[length:]
	}
	return out, nil
}

// rangeToPrefixes splits an inclusive address range into CIDR prefixes.
func rangeToPrefixes(start, end netip.Addr) []netip.Prefix {
	var out []netip.Prefix
	for start.Compare(end) <= 0 {
		bits := start.BitLen()
		prefix := netip.PrefixFrom(start, bits)
		for bits > 0 {
			candidate := netip.PrefixFrom(start, bits-1).Masked()
			if candidate.Addr() != start || lastAddr(candidate).Compare(end) > 0 {
				break
			}
			bits--
			prefix = candidate
		}
		out = append(out, prefix)
		next := lastAddr(prefix).Next()
		if !next.IsValid() {
			break
		}
		start = next
	}
	return out
}

func lastAddr(p netip.Prefix) netip.Addr {
	b := p.Addr().AsSlice()
	for i := p.Bits(); i < len(b)*8; i++ {
		b[i/8] |= 0x80 >> (i % 8)
	}
	a, _ := netip.AddrFromSlice(b)
	return a
}

func encodeCPRequest() []byte {
	return []byte{
		cfgRequest, 0, 0, 0,
		0, attrInternalIP4Address, 0, 0,
		0, attrInternalIP4DNS, 0, 0,
	}
}

// parseCPReply extracts the assigned addresses and DNS servers from a configuration reply.
func parseCPReply(b []byte) (addrs, dns []netip.Addr, err error) {
	if len(b) < 4 {
		return nil, nil, errShortPacket
	}
	if b[0] != cfgReply {
		return nil, nil, fmt.Errorf("ikev2: unexpected configuration payload type %d", b[0])
	}
	for rest := b[4:]; len(rest) >= 4; {
		typ := binary.BigEndian.Uint16(rest) & 0x7fff
		length := int(binary.BigEndian.Uint16(rest[2:]))
		if 4+length > len(rest) {
			return nil, nil, errors.New("ikev2: bad configuration attribute")
		}
		value := rest[4 : 4+length]
		switch {
		case typ == attrInternalIP4Address && length == 4, typ == attrInternalIP6Address && length == 17:
			a, _ := netip.AddrFromSlice(value[:length/4*4])
			addrs = append(addrs, a)
		case typ == attrInternalIP4DNS && length == 4, typ == attrInternalIP6DNS && length == 16:
			a, _ := netip.AddrFromSlice(value)
			dns = append(dns, a)
		}
		rest = rest[4+length:]
	}
	return addrs, dns, nil
}

func concat(parts ...[]byte) []byte {
	var n int
	for _, p := range parts {
		n += len(p)
	}
	out := make([]byte, 0, n)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}
