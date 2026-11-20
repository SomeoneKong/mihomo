package ikev2

import (
	"context"
	"crypto/hmac"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/log"
)

// DialFunc opens a UDP flow to the gateway on the given port.
type DialFunc func(ctx context.Context, port uint16) (net.Conn, error)

// Lease is what the gateway assigned through the configuration payload and the traffic selectors.
type Lease struct {
	Addresses []netip.Addr
	DNS       []netip.Addr
	Routes    []netip.Prefix
}

const (
	natTPort             = 4500
	nonceLen             = 32
	natKeepaliveInterval = 20 * time.Second
	retiredSAGrace       = 10 * time.Second
	rekeyRetryDelay      = 10 * time.Second
	packetQueueLen       = 1024

	rekeyChild = 1
	rekeyIKE   = 2
)

var (
	retransmitTimeouts = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second}
	keyPad             = []byte("Key Pad for IKEv2")
)

// Client is an IKEv2 initiator with a userspace ESP data path. After Handshake it carries IP packets
// through ReadIPPacket and WriteIPPacket and keeps the SAs alive with DPD, NAT keepalives and rekeying.
// Rekeying is always initiated by the client: a rekey requested by the gateway is answered with
// TEMPORARY_FAILURE and followed by a client initiated rekey.
type Client struct {
	cfg  *Config
	dial DialFunc
	conn net.Conn

	mu         sync.Mutex
	ike        *ikeSA
	oldIKE     *ikeSA
	child      *childSA
	childAt    time.Time
	retiring   *childSA
	retireAt   time.Time
	deferRekey time.Time
	tsI, tsR   []byte

	out   atomic.Pointer[espSA]
	inCur atomic.Pointer[espSA]
	inOld atomic.Pointer[espSA]

	packets   chan []byte
	rekeyReq  chan rekeyRequest
	closed    chan struct{}
	closeOnce sync.Once
	closeErr  error

	lastSend atomic.Int64
	lastRecv atomic.Int64
}

type ikeSA struct {
	*ikeKeys
	spiI, spiR uint64
	created    time.Time

	mu         sync.Mutex
	nextOut    uint32
	nextIn     uint32
	waiters    map[uint32]chan *message
	lastResp   []byte
	lastRespID uint32
}

func newIKESA(keys *ikeKeys, spiI, spiR uint64) *ikeSA {
	return &ikeSA{ikeKeys: keys, spiI: spiI, spiR: spiR, created: time.Now(), waiters: map[uint32]chan *message{}}
}

// NewClient creates a client from a prepared Config.
func NewClient(cfg *Config, dial DialFunc) (*Client, error) {
	if len(cfg.ikeSuites) == 0 || len(cfg.espSuites) == 0 {
		return nil, errors.New("ikev2: config is not prepared")
	}
	return &Client{
		cfg:      cfg,
		dial:     dial,
		packets:  make(chan []byte, packetQueueLen),
		rekeyReq: make(chan rekeyRequest, 4),
		closed:   make(chan struct{}),
	}, nil
}

// Handshake establishes the IKE SA and the first CHILD SA.
func (c *Client) Handshake(ctx context.Context) (*Lease, error) {
	init, err := c.ikeSAInit(ctx)
	if err != nil {
		c.shutdown(err, false)
		return nil, err
	}
	go c.readLoop()
	lease, err := c.ikeAuth(ctx, init)
	if err != nil {
		c.shutdown(err, false)
		return nil, err
	}
	go c.controlLoop()
	return lease, nil
}

type initResult struct {
	sa                *ikeSA
	ni, nr            []byte
	request, response []byte
}

func (c *Client) ikeSAInit(ctx context.Context) (*initResult, error) {
	conn, err := c.dial(ctx, c.cfg.Port)
	if err != nil {
		return nil, err
	}
	// Only port 500 carries IKE without the non-ESP marker.
	marker := c.cfg.Port != DefaultPort
	spiI := randomSPI64()
	ni := randomBytes(nonceLen)
	group := c.cfg.ikeSuites[0].dh
	var cookie []byte
	for attempt := 0; attempt < 4; attempt++ {
		priv, err := group.generate()
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		var payloads []payload
		if cookie != nil {
			payloads = append(payloads, payload{payloadN, encodeNotify(0, notifyCookie, nil, cookie)})
		}
		props := make([]proposal, len(c.cfg.ikeSuites))
		for i, s := range c.cfg.ikeSuites {
			props[i] = proposal{num: uint8(i + 1), proto: protoIKE, transforms: s.transforms(true, true)}
		}
		payloads = append(payloads,
			payload{payloadSA, encodeSA(props)},
			payload{payloadKE, encodeKE(group.id, priv.pub)},
			payload{payloadNonce, ni},
			// Random NAT detection hashes make the gateway see the client behind a NAT, so ESP is always
			// encapsulated in UDP and can go through the dialer.
			payload{payloadN, encodeNotify(0, notifyNATDetectionSourceIP, nil, randomBytes(20))},
			payload{payloadN, encodeNotify(0, notifyNATDetectionDestIP, nil, randomBytes(20))},
		)
		req := (&message{spiI: spiI, exchange: exchangeIKESAInit, flags: flagInitiator, payloads: payloads}).encodePlain()
		raw, err := exchangePlain(ctx, conn, marker, req, spiI)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		resp, _, _, err := parseMessage(raw)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		if n := resp.notify(notifyCookie); n != nil {
			cookie = append([]byte(nil), n.data...)
			continue
		}
		if n := resp.notify(notifyInvalidKEPayload); n != nil && len(n.data) >= 2 {
			if group = offeredGroup(c.cfg.ikeSuites, binary.BigEndian.Uint16(n.data)); group == nil {
				_ = conn.Close()
				return nil, fmt.Errorf("ikev2: gateway requires DH group %d, which is not configured", binary.BigEndian.Uint16(n.data))
			}
			continue
		}
		res, err := c.finishInit(resp, raw, req, priv, group, ni)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		if c.cfg.Port == DefaultPort {
			_ = conn.Close()
			if conn, err = c.dial(ctx, natTPort); err != nil {
				return nil, err
			}
		}
		c.conn = conn
		return res, nil
	}
	_ = conn.Close()
	return nil, errors.New("ikev2: IKE_SA_INIT did not complete")
}

func (c *Client) finishInit(resp *message, raw, req []byte, priv *dhPrivate, group *dhGroup, ni []byte) (*initResult, error) {
	if err := resp.errorNotify(); err != nil {
		return nil, err
	}
	prop, err := chosenProposal(resp)
	if err != nil {
		return nil, err
	}
	var s *suite
	for _, candidate := range c.cfg.ikeSuites {
		if candidate.dh == group && candidate.matches(prop.transforms, true, true) {
			s = candidate
			break
		}
	}
	if s == nil {
		return nil, errors.New("ikev2: gateway chose an IKE proposal that was not offered")
	}
	keGroup, pub, err := parseKE(resp.find(payloadKE))
	if err != nil || keGroup != group.id {
		return nil, errors.New("ikev2: bad key exchange payload from gateway")
	}
	nr := resp.find(payloadNonce)
	if len(nr) < 16 || len(nr) > 256 {
		return nil, errors.New("ikev2: bad nonce from gateway")
	}
	shared, err := priv.shared(pub)
	if err != nil {
		return nil, err
	}
	skeyseed := s.integ.prf(concat(ni, nr), shared)
	keys, err := deriveIKEKeys(s, skeyseed, ni, nr, resp.spiI, resp.spiR)
	if err != nil {
		return nil, err
	}
	log.Debugln("[IKEv2] IKE_SA_INIT done with %s", s)
	sa := newIKESA(keys, resp.spiI, resp.spiR)
	// Our IKE_SA_INIT request used message ID 0; requests from the gateway count separately from 0.
	sa.nextOut = 1
	return &initResult{sa: sa, ni: ni, nr: nr, request: req, response: raw}, nil
}

func offeredGroup(suites []*suite, id uint16) *dhGroup {
	for _, s := range suites {
		if s.dh.id == id {
			return s.dh
		}
	}
	return nil
}

// exchangePlain sends IKE_SA_INIT and waits for its response, retransmitting on timeout.
func exchangePlain(ctx context.Context, conn net.Conn, marker bool, req []byte, spiI uint64) ([]byte, error) {
	out := req
	if marker {
		out = append(make([]byte, 4, 4+len(req)), req...)
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.SetReadDeadline(time.Now())
		case <-done:
		}
	}()
	defer conn.SetReadDeadline(time.Time{})
	buf := make([]byte, 65535)
	for _, timeout := range retransmitTimeouts {
		if _, err := conn.Write(out); err != nil {
			return nil, err
		}
		if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return nil, err
		}
		for {
			n, err := conn.Read(buf)
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if err != nil {
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					break
				}
				return nil, err
			}
			b := buf[:n]
			if marker {
				if n < 4 || binary.BigEndian.Uint32(b) != 0 {
					continue
				}
				b = b[4:]
			}
			if len(b) < headerLen || binary.BigEndian.Uint64(b) != spiI || b[18] != exchangeIKESAInit ||
				b[19]&flagResponse == 0 || binary.BigEndian.Uint32(b[20:]) != 0 {
				continue
			}
			return append([]byte(nil), b...), nil
		}
	}
	return nil, errors.New("ikev2: no response to IKE_SA_INIT")
}

func (c *Client) ikeAuth(ctx context.Context, in *initResult) (*Lease, error) {
	sa := in.sa
	prf := sa.suite.integ
	c.mu.Lock()
	c.ike = sa
	c.mu.Unlock()

	idi := c.localID()
	inSPI := randomSPI32()
	props := make([]proposal, len(c.cfg.espSuites))
	for i, s := range c.cfg.espSuites {
		props[i] = proposal{num: uint8(i + 1), proto: protoESP, spi: be32(inSPI), transforms: s.transforms(false, false)}
	}
	useEAP := c.cfg.Username != ""
	payloads := []payload{{payloadIDi, idi}}
	if !useEAP {
		payloads = append(payloads, payload{payloadAUTH, encodeAuth(authValue(prf, []byte(c.cfg.PSK), in.request, in.nr, sa.skPI, idi))})
	}
	payloads = append(payloads,
		payload{payloadCP, encodeCPRequest()},
		payload{payloadSA, encodeSA(props)},
		payload{payloadTSi, encodeTSAny()},
		payload{payloadTSr, encodeTSAny()},
	)
	resp, err := c.checkedRequest(ctx, sa, exchangeIKEAuth, payloads)
	if err != nil {
		return nil, err
	}
	idr := resp.find(payloadIDr)
	if idr == nil {
		return nil, errors.New("ikev2: gateway sent no identity")
	}
	if err := verifyAuth(resp, prf, []byte(c.cfg.PSK), in.response, in.ni, sa.skPR, idr); err != nil {
		return nil, fmt.Errorf("%w, check the pre-shared key", err)
	}
	if useEAP {
		peer := &eapPeer{username: c.cfg.Username, password: c.cfg.Password}
		for {
			req := resp.find(payloadEAP)
			if len(req) < 4 {
				return nil, errors.New("ikev2: gateway did not continue EAP")
			}
			if req[0] == eapCodeSuccess {
				break
			}
			if req[0] == eapCodeFailure {
				return nil, errors.New("ikev2: EAP authentication failed, check the username and password")
			}
			out, err := peer.respond(req)
			if err != nil {
				return nil, err
			}
			if resp, err = c.checkedRequest(ctx, sa, exchangeIKEAuth, []payload{{payloadEAP, out}}); err != nil {
				return nil, err
			}
		}
		if peer.msk == nil {
			return nil, errors.New("ikev2: EAP finished without a key")
		}
		auth := authValue(prf, peer.msk, in.request, in.nr, sa.skPI, idi)
		if resp, err = c.checkedRequest(ctx, sa, exchangeIKEAuth, []payload{{payloadAUTH, encodeAuth(auth)}}); err != nil {
			return nil, err
		}
		if v := resp.find(payloadIDr); v != nil {
			idr = v
		}
		if err := verifyAuth(resp, prf, peer.msk, in.response, in.ni, sa.skPR, idr); err != nil {
			return nil, err
		}
	}
	return c.installFirstChild(resp, in, inSPI)
}

func (c *Client) installFirstChild(resp *message, in *initResult, inSPI uint32) (*Lease, error) {
	prop, err := chosenProposal(resp)
	if err != nil {
		return nil, err
	}
	if prop.proto != protoESP || len(prop.spi) != 4 {
		return nil, errors.New("ikev2: bad CHILD SA proposal from gateway")
	}
	var s *suite
	for _, candidate := range c.cfg.espSuites {
		if candidate.matches(prop.transforms, false, false) {
			s = candidate
			break
		}
	}
	if s == nil {
		return nil, errors.New("ikev2: gateway chose an ESP proposal that was not offered")
	}
	cp := resp.find(payloadCP)
	if cp == nil {
		return nil, errors.New("ikev2: gateway assigned no address")
	}
	addrs, dns, err := parseCPReply(cp)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errors.New("ikev2: gateway assigned no address")
	}
	sa := in.sa
	child, err := newChildSA(sa.suite.integ, sa.skD, s, inSPI, binary.BigEndian.Uint32(prop.spi), concat(in.ni, in.nr))
	if err != nil {
		return nil, err
	}
	lease := &Lease{Addresses: addrs, DNS: dns}
	c.mu.Lock()
	c.tsI, c.tsR = encodeTSAny(), encodeTSAny()
	if ts := resp.find(payloadTSi); ts != nil {
		c.tsI = ts
	}
	if ts := resp.find(payloadTSr); ts != nil {
		c.tsR = ts
		lease.Routes, _ = parseTS(ts)
	}
	c.child, c.childAt = child, time.Now()
	c.mu.Unlock()
	c.inCur.Store(child.in)
	c.out.Store(child.out)
	now := time.Now().UnixNano()
	c.lastSend.Store(now)
	c.lastRecv.Store(now)
	log.Debugln("[IKEv2] CHILD SA established with %s, addresses %v, dns %v, routes %v", s, addrs, dns, lease.Routes)
	return lease, nil
}

// authValue is the shared key AUTH value of RFC 7296 section 2.15.
func authValue(prf *hashAlg, secret, signedMessage, nonce, skP, idBody []byte) []byte {
	return prf.prf(prf.prf(secret, keyPad), signedMessage, nonce, prf.prf(skP, idBody))
}

func verifyAuth(resp *message, prf *hashAlg, secret, signedMessage, nonce, skP, idBody []byte) error {
	auth := resp.find(payloadAUTH)
	if len(auth) < 4 {
		return errors.New("ikev2: gateway sent no AUTH payload")
	}
	if auth[0] != authSharedKey {
		return fmt.Errorf("ikev2: unsupported gateway authentication method %d", auth[0])
	}
	if !hmac.Equal(auth[4:], authValue(prf, secret, signedMessage, nonce, skP, idBody)) {
		return errors.New("ikev2: gateway authentication failed")
	}
	return nil
}

func (c *Client) localID() []byte {
	if id := c.cfg.LocalID; id != "" {
		if ip, err := netip.ParseAddr(id); err == nil {
			ip = ip.Unmap()
			if ip.Is4() {
				return encodeID(idIPv4, ip.AsSlice())
			}
			return encodeID(idIPv6, ip.AsSlice())
		}
		if strings.Contains(id, "@") {
			return encodeID(idEmail, []byte(id))
		}
		return encodeID(idFQDN, []byte(id))
	}
	if addr, ok := c.conn.LocalAddr().(*net.UDPAddr); ok {
		if ip4 := addr.IP.To4(); ip4 != nil && !ip4.IsUnspecified() {
			return encodeID(idIPv4, ip4)
		}
	}
	return encodeID(idKeyID, []byte("mihomo"))
}

// request sends an encrypted request on sa and waits for its response, retransmitting on timeout.
func (c *Client) request(ctx context.Context, sa *ikeSA, exchange uint8, payloads []payload) (*message, error) {
	ch := make(chan *message, 1)
	sa.mu.Lock()
	id := sa.nextOut
	sa.nextOut++
	sa.waiters[id] = ch
	sa.mu.Unlock()
	defer func() {
		sa.mu.Lock()
		delete(sa.waiters, id)
		sa.mu.Unlock()
	}()
	raw := sa.seal(&message{spiI: sa.spiI, spiR: sa.spiR, exchange: exchange, flags: flagInitiator, msgID: id, payloads: payloads})
	for _, timeout := range retransmitTimeouts {
		if err := c.sendIKE(raw); err != nil {
			return nil, err
		}
		timer := time.NewTimer(timeout)
		select {
		case m := <-ch:
			timer.Stop()
			return m, nil
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-c.closed:
			timer.Stop()
			return nil, c.closeErr
		}
	}
	return nil, errors.New("ikev2: gateway did not respond")
}

func (c *Client) checkedRequest(ctx context.Context, sa *ikeSA, exchange uint8, payloads []payload) (*message, error) {
	m, err := c.request(ctx, sa, exchange, payloads)
	if err != nil {
		return nil, err
	}
	if err := m.errorNotify(); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *Client) sendIKE(raw []byte) error {
	if _, err := c.conn.Write(append(make([]byte, 4, 4+len(raw)), raw...)); err != nil {
		return err
	}
	c.lastSend.Store(time.Now().UnixNano())
	return nil
}

func (c *Client) readLoop() {
	buf := make([]byte, 65535)
	for {
		n, err := c.conn.Read(buf)
		if err != nil {
			c.shutdown(fmt.Errorf("ikev2: read from gateway: %w", err), false)
			return
		}
		b := buf[:n]
		switch {
		case n == 1 && b[0] == 0xff:
			// NAT keepalive (RFC 3948 section 2.3).
		case n >= 4 && binary.BigEndian.Uint32(b) == 0:
			c.handleIKE(append([]byte(nil), b[4:]...))
		case n >= 8:
			c.handleESP(b)
		}
	}
}

func (c *Client) handleESP(b []byte) {
	spi := binary.BigEndian.Uint32(b)
	sa := c.inCur.Load()
	if sa == nil || sa.spi != spi {
		if sa = c.inOld.Load(); sa == nil || sa.spi != spi {
			return
		}
	}
	packet, err := sa.open(b)
	if err != nil {
		if !errors.Is(err, errReplay) {
			log.Debugln("[IKEv2] drop ESP packet: %v", err)
		}
		return
	}
	c.lastRecv.Store(time.Now().UnixNano())
	if packet == nil {
		return
	}
	select {
	case c.packets <- packet:
	default:
	}
}

func (c *Client) handleIKE(raw []byte) {
	if len(raw) < headerLen {
		return
	}
	spiI, spiR := binary.BigEndian.Uint64(raw), binary.BigEndian.Uint64(raw[8:])
	c.mu.Lock()
	var sa *ikeSA
	for _, candidate := range []*ikeSA{c.ike, c.oldIKE} {
		if candidate != nil && candidate.spiI == spiI && candidate.spiR == spiR {
			sa = candidate
			break
		}
	}
	c.mu.Unlock()
	if sa == nil {
		return
	}
	m, err := sa.open(raw)
	if err != nil {
		log.Debugln("[IKEv2] drop IKE message: %v", err)
		return
	}
	c.lastRecv.Store(time.Now().UnixNano())
	if m.isResponse() {
		sa.mu.Lock()
		ch := sa.waiters[m.msgID]
		sa.mu.Unlock()
		if ch != nil {
			select {
			case ch <- m:
			default:
			}
		}
		return
	}
	c.handlePeerRequest(sa, m)
}

// handlePeerRequest answers a request from the gateway. It runs on the read loop only.
func (c *Client) handlePeerRequest(sa *ikeSA, m *message) {
	sa.mu.Lock()
	if sa.lastResp != nil && m.msgID == sa.lastRespID {
		resp := sa.lastResp
		sa.mu.Unlock()
		_ = c.sendIKE(resp)
		return
	}
	if m.msgID != sa.nextIn {
		sa.mu.Unlock()
		return
	}
	sa.nextIn++
	sa.mu.Unlock()

	var out []payload
	var after func()
	switch m.exchange {
	case exchangeInformational:
		out, after = c.onInformational(sa, m)
	case exchangeCreateChildSA:
		out = c.onCreateChildSA(sa, m)
	default:
		return
	}
	resp := sa.seal(&message{spiI: sa.spiI, spiR: sa.spiR, exchange: m.exchange, flags: flagInitiator | flagResponse, msgID: m.msgID, payloads: out})
	sa.mu.Lock()
	sa.lastResp, sa.lastRespID = resp, m.msgID
	sa.mu.Unlock()
	_ = c.sendIKE(resp)
	if after != nil {
		after()
	}
}

func (c *Client) onInformational(sa *ikeSA, m *message) (out []payload, after func()) {
	for _, p := range m.payloads {
		if p.typ != payloadD {
			continue
		}
		proto, spis, err := parseDelete(p.body)
		if err != nil {
			continue
		}
		c.mu.Lock()
		current, child, retiring := c.ike, c.child, c.retiring
		if proto == protoIKE && sa != current && sa == c.oldIKE {
			c.oldIKE = nil
		}
		c.mu.Unlock()
		switch proto {
		case protoIKE:
			if sa == current {
				after = func() { c.shutdown(errors.New("ikev2: gateway deleted the IKE SA"), false) }
			}
		case protoESP:
			for _, spi := range spis {
				switch {
				case child != nil && child.out.spi == spi:
					out = append(out, payload{payloadD, encodeDelete(protoESP, child.in.spi)})
					after = func() { c.shutdown(errors.New("ikev2: gateway deleted the CHILD SA"), false) }
				case retiring != nil && retiring.out.spi == spi:
					out = append(out, payload{payloadD, encodeDelete(protoESP, retiring.in.spi)})
					c.retire(retiring)
				}
			}
		}
	}
	return out, after
}

// rekeyRequest is a rekey the gateway asked for, naming the SA to rekey: the IKE SA it arrived on,
// or the CHILD SA by the SPI the client sends with.
type rekeyRequest struct {
	kind int
	ike  *ikeSA
	spi  uint32
}

func (c *Client) onCreateChildSA(sa *ikeSA, m *message) []payload {
	r := rekeyRequest{ike: sa}
	if n := m.notify(notifyRekeySA); n != nil && n.proto == protoESP && len(n.spi) == 4 {
		r.kind, r.spi = rekeyChild, binary.BigEndian.Uint32(n.spi)
	} else if props, err := parseSA(m.find(payloadSA)); err == nil && len(props) > 0 && props[0].proto == protoIKE {
		r.kind = rekeyIKE
	}
	if r.kind == 0 {
		return []payload{{payloadN, encodeNotify(0, notifyNoAdditionalSAs, nil, nil)}}
	}
	select {
	case c.rekeyReq <- r:
	default:
	}
	return []payload{{payloadN, encodeNotify(0, notifyTemporaryFailure, nil, nil)}}
}

// pending reports whether the SA named by a gateway rekey request has not been rekeyed yet.
// A gateway may repeat its request right after TEMPORARY_FAILURE.
func (c *Client) pending(r rekeyRequest) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.kind == rekeyIKE {
		return c.ike == r.ike
	}
	return c.child != nil && c.child.out.spi == r.spi
}

func (c *Client) controlLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		var err error
		select {
		case <-c.closed:
			return
		case r := <-c.rekeyReq:
			if c.pending(r) {
				err = c.rekey(r.kind)
			}
		case now := <-ticker.C:
			err = c.tick(now)
		}
		if err != nil {
			c.shutdown(err, true)
			return
		}
	}
}

func rekeyTime(lifetime time.Duration) time.Duration {
	return lifetime - lifetime/10
}

func (c *Client) tick(now time.Time) error {
	c.mu.Lock()
	ike, child, childAt, retiring, retireAt, deferRekey := c.ike, c.child, c.childAt, c.retiring, c.retireAt, c.deferRekey
	c.mu.Unlock()
	if retiring != nil && now.After(retireAt) {
		c.retire(retiring)
	}
	if now.After(deferRekey) {
		if now.Sub(childAt) >= rekeyTime(c.cfg.ESPLifetime) || child.out.seq.Load() >= seqRekeyThreshold {
			return c.rekey(rekeyChild)
		}
		if now.Sub(ike.created) >= rekeyTime(c.cfg.IKELifetime) {
			return c.rekey(rekeyIKE)
		}
	}
	if now.Sub(time.Unix(0, c.lastRecv.Load())) >= c.cfg.DPDInterval {
		if _, err := c.request(context.Background(), ike, exchangeInformational, nil); err != nil {
			return fmt.Errorf("ikev2: dead peer detection failed: %w", err)
		}
		return nil
	}
	if now.Sub(time.Unix(0, c.lastSend.Load())) >= natKeepaliveInterval {
		if _, err := c.conn.Write([]byte{0xff}); err == nil {
			c.lastSend.Store(now.UnixNano())
		}
	}
	return nil
}

func (c *Client) rekey(kind int) error {
	var err error
	if kind == rekeyIKE {
		err = c.rekeyIKE()
	} else {
		err = c.rekeyChild()
	}
	var ne *NotifyError
	if errors.As(err, &ne) && ne.Type == notifyTemporaryFailure {
		c.mu.Lock()
		c.deferRekey = time.Now().Add(rekeyRetryDelay)
		c.mu.Unlock()
		return nil
	}
	return err
}

func (c *Client) rekeyChild() error {
	c.mu.Lock()
	ike, old, tsI, tsR := c.ike, c.child, c.tsI, c.tsR
	c.mu.Unlock()
	s := old.suite
	inSPI := randomSPI32()
	ni := randomBytes(nonceLen)
	payloads := []payload{
		{payloadN, encodeNotify(protoESP, notifyRekeySA, be32(old.in.spi), nil)},
		{payloadSA, encodeSA([]proposal{{num: 1, proto: protoESP, spi: be32(inSPI), transforms: s.transforms(false, true)}})},
		{payloadNonce, ni},
	}
	var priv *dhPrivate
	if s.dh != nil {
		var err error
		if priv, err = s.dh.generate(); err != nil {
			return err
		}
		payloads = append(payloads, payload{payloadKE, encodeKE(s.dh.id, priv.pub)})
	}
	payloads = append(payloads, payload{payloadTSi, tsI}, payload{payloadTSr, tsR})
	resp, err := c.checkedRequest(context.Background(), ike, exchangeCreateChildSA, payloads)
	if err != nil {
		return fmt.Errorf("ikev2: rekey CHILD SA: %w", err)
	}
	prop, err := chosenProposal(resp)
	if err != nil {
		return err
	}
	if prop.proto != protoESP || len(prop.spi) != 4 || !s.matches(prop.transforms, false, true) {
		return errors.New("ikev2: bad proposal in CHILD SA rekey response")
	}
	nr := resp.find(payloadNonce)
	if len(nr) < 16 {
		return errors.New("ikev2: bad nonce in CHILD SA rekey response")
	}
	seed := concat(ni, nr)
	if priv != nil {
		group, pub, err := parseKE(resp.find(payloadKE))
		if err != nil || group != s.dh.id {
			return errors.New("ikev2: bad key exchange in CHILD SA rekey response")
		}
		shared, err := priv.shared(pub)
		if err != nil {
			return err
		}
		seed = concat(shared, ni, nr)
	}
	child, err := newChildSA(ike.suite.integ, ike.skD, s, inSPI, binary.BigEndian.Uint32(prop.spi), seed)
	if err != nil {
		return err
	}
	now := time.Now()
	c.mu.Lock()
	c.child, c.childAt = child, now
	c.retiring, c.retireAt = old, now.Add(retiredSAGrace)
	c.mu.Unlock()
	c.inOld.Store(old.in)
	c.inCur.Store(child.in)
	c.out.Store(child.out)
	log.Debugln("[IKEv2] CHILD SA rekeyed, SPIs %08x/%08x", child.in.spi, child.out.spi)
	if _, err := c.request(context.Background(), ike, exchangeInformational, []payload{{payloadD, encodeDelete(protoESP, old.in.spi)}}); err != nil {
		return fmt.Errorf("ikev2: delete old CHILD SA: %w", err)
	}
	return nil
}

func (c *Client) retire(child *childSA) {
	c.mu.Lock()
	if c.retiring == child {
		c.retiring = nil
	}
	c.mu.Unlock()
	c.inOld.CompareAndSwap(child.in, nil)
}

func (c *Client) rekeyIKE() error {
	c.mu.Lock()
	old := c.ike
	c.mu.Unlock()
	s := old.suite
	spiI := randomSPI64()
	ni := randomBytes(nonceLen)
	priv, err := s.dh.generate()
	if err != nil {
		return err
	}
	resp, err := c.checkedRequest(context.Background(), old, exchangeCreateChildSA, []payload{
		{payloadSA, encodeSA([]proposal{{num: 1, proto: protoIKE, spi: be64(spiI), transforms: s.transforms(true, true)}})},
		{payloadNonce, ni},
		{payloadKE, encodeKE(s.dh.id, priv.pub)},
	})
	if err != nil {
		return fmt.Errorf("ikev2: rekey IKE SA: %w", err)
	}
	prop, err := chosenProposal(resp)
	if err != nil {
		return err
	}
	if prop.proto != protoIKE || len(prop.spi) != 8 || !s.matches(prop.transforms, true, true) {
		return errors.New("ikev2: bad proposal in IKE SA rekey response")
	}
	nr := resp.find(payloadNonce)
	group, pub, err := parseKE(resp.find(payloadKE))
	if len(nr) < 16 || err != nil || group != s.dh.id {
		return errors.New("ikev2: bad IKE SA rekey response")
	}
	shared, err := priv.shared(pub)
	if err != nil {
		return err
	}
	// RFC 7296 section 2.18: SKEYSEED = prf(SK_d (old), g^ir (new) | Ni | Nr).
	skeyseed := s.integ.prf(old.skD, shared, ni, nr)
	spiR := binary.BigEndian.Uint64(prop.spi)
	keys, err := deriveIKEKeys(s, skeyseed, ni, nr, spiI, spiR)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.ike, c.oldIKE = newIKESA(keys, spiI, spiR), old
	c.mu.Unlock()
	log.Debugln("[IKEv2] IKE SA rekeyed")
	_, err = c.request(context.Background(), old, exchangeInformational, []payload{{payloadD, encodeDelete(protoIKE)}})
	c.mu.Lock()
	if c.oldIKE == old {
		c.oldIKE = nil
	}
	c.mu.Unlock()
	if err != nil {
		log.Debugln("[IKEv2] delete old IKE SA: %v", err)
	}
	return nil
}

// ReadIPPacket returns the next IP packet received through the tunnel.
func (c *Client) ReadIPPacket(ctx context.Context) ([]byte, error) {
	select {
	case p := <-c.packets:
		return p, nil
	case <-c.closed:
		return nil, c.closeErr
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// WriteIPPacket sends an IP packet through the tunnel.
func (c *Client) WriteIPPacket(ctx context.Context, packet []byte) error {
	select {
	case <-c.closed:
		return c.closeErr
	default:
	}
	sa := c.out.Load()
	if sa == nil {
		return errors.New("ikev2: tunnel is not established")
	}
	b, err := sa.seal(packet)
	if err != nil {
		return err
	}
	if _, err := c.conn.Write(b); err != nil {
		return err
	}
	c.lastSend.Store(time.Now().UnixNano())
	return nil
}

// Close deletes the IKE SA on the gateway and releases the connection.
func (c *Client) Close() error {
	c.shutdown(net.ErrClosed, true)
	return nil
}

func (c *Client) shutdown(err error, notifyPeer bool) {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		sa, established := c.ike, c.child != nil
		c.mu.Unlock()
		if notifyPeer && established {
			sa.mu.Lock()
			id := sa.nextOut
			sa.nextOut++
			sa.mu.Unlock()
			_ = c.sendIKE(sa.seal(&message{
				spiI: sa.spiI, spiR: sa.spiR, exchange: exchangeInformational, flags: flagInitiator, msgID: id,
				payloads: []payload{{payloadD, encodeDelete(protoIKE)}},
			}))
		}
		if !errors.Is(err, net.ErrClosed) {
			log.Debugln("[IKEv2] tunnel closed: %v", err)
		}
		c.closeErr = err
		close(c.closed)
		if c.conn != nil {
			_ = c.conn.Close()
		}
	})
}

func be32(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }

func be64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }
