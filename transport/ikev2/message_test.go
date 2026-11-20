package ikev2

import (
	"bytes"
	"net/netip"
	"reflect"
	"testing"
)

func TestSARoundTrip(t *testing.T) {
	s, err := parseSuite("aes256-sha256-ecp384")
	if err != nil {
		t.Fatal(err)
	}
	props := []proposal{
		{num: 1, proto: protoIKE, transforms: s.transforms(true, true)},
		{num: 2, proto: protoESP, spi: []byte{1, 2, 3, 4}, transforms: s.transforms(false, false)},
	}
	got, err := parseSA(encodeSA(props))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d proposals", len(got))
	}
	for i := range props {
		if got[i].num != props[i].num || got[i].proto != props[i].proto || !bytes.Equal(got[i].spi, props[i].spi) ||
			!reflect.DeepEqual(got[i].transforms, props[i].transforms) {
			t.Errorf("proposal %d = %+v, want %+v", i, got[i], props[i])
		}
	}
	if !s.matches(got[0].transforms, true, true) || s.matches(got[0].transforms, false, false) {
		t.Error("suite matching is wrong")
	}
	if !s.matches(got[1].transforms, false, false) || s.matches(got[1].transforms, false, true) {
		t.Error("ESP suite matching is wrong")
	}
}

func TestSealOpen(t *testing.T) {
	s, err := parseSuite("aes128-sha1-modp2048")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := deriveIKEKeys(s, randomBytes(20), randomBytes(32), randomBytes(32), 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	// Decrypting our own message needs the responder keys to equal the initiator keys.
	peer := *keys
	peer.encR, peer.skAR = keys.encI, keys.skAI

	for _, payloads := range [][]payload{nil, {{payloadEAP, []byte("x")}}, {{payloadIDi, encodeID(idFQDN, []byte("client"))}, {payloadN, encodeNotify(0, notifyCookie, nil, bytes.Repeat([]byte{9}, 31))}}} {
		m := &message{spiI: 1, spiR: 2, exchange: exchangeIKEAuth, flags: flagInitiator, msgID: 5, payloads: payloads}
		raw := keys.seal(m)
		got, err := peer.open(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got.spiI != 1 || got.spiR != 2 || got.msgID != 5 || got.exchange != exchangeIKEAuth || !reflect.DeepEqual(got.payloads, payloads) {
			t.Fatalf("got %+v, want %+v", got, m)
		}
		raw[len(raw)-1] ^= 1
		if _, err := peer.open(raw); err == nil {
			t.Fatal("tampered message accepted")
		}
	}
}

func TestPlainMessage(t *testing.T) {
	m := &message{spiI: 7, exchange: exchangeIKESAInit, flags: flagInitiator, payloads: []payload{
		{payloadNonce, []byte("nonce")},
		{payloadN, encodeNotify(0, notifyInvalidKEPayload, nil, []byte{0, 20})},
	}}
	got, _, sk, err := parseMessage(m.encodePlain())
	if err != nil || sk != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.payloads, m.payloads) {
		t.Fatalf("payloads = %+v", got.payloads)
	}
	n := got.notify(notifyInvalidKEPayload)
	if n == nil || !bytes.Equal(n.data, []byte{0, 20}) {
		t.Fatalf("notify = %+v", n)
	}
	if err := got.errorNotify(); err == nil || err.(*NotifyError).Name != "INVALID_KE_PAYLOAD" {
		t.Fatalf("error notify = %v", err)
	}
}

func TestTrafficSelectors(t *testing.T) {
	prefixes, err := parseTS(encodeTSAny())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(prefixes, []netip.Prefix{netip.MustParsePrefix("0.0.0.0/0")}) {
		t.Fatalf("any = %v", prefixes)
	}
	got := rangeToPrefixes(netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("10.0.0.6"))
	want := []netip.Prefix{
		netip.MustParsePrefix("10.0.0.1/32"), netip.MustParsePrefix("10.0.0.2/31"),
		netip.MustParsePrefix("10.0.0.4/31"), netip.MustParsePrefix("10.0.0.6/32"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("range = %v", got)
	}
}

func TestCPReply(t *testing.T) {
	body := []byte{
		cfgReply, 0, 0, 0,
		0, attrInternalIP4Address, 0, 4, 172, 31, 4, 1,
		0, attrInternalIP4DNS, 0, 4, 75, 75, 75, 75,
		0, attrInternalIP4DNS, 0, 4, 75, 75, 76, 76,
		0, 99, 0, 2, 1, 2,
	}
	addrs, dns, err := parseCPReply(body)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(addrs, []netip.Addr{netip.MustParseAddr("172.31.4.1")}) ||
		!reflect.DeepEqual(dns, []netip.Addr{netip.MustParseAddr("75.75.75.75"), netip.MustParseAddr("75.75.76.76")}) {
		t.Fatalf("addrs %v dns %v", addrs, dns)
	}
}

func TestDelete(t *testing.T) {
	proto, spis, err := parseDelete(encodeDelete(protoESP, 0x01020304, 0x0a0b0c0d))
	if err != nil || proto != protoESP || !reflect.DeepEqual(spis, []uint32{0x01020304, 0x0a0b0c0d}) {
		t.Fatalf("%d %x %v", proto, spis, err)
	}
	if proto, _, err := parseDelete(encodeDelete(protoIKE)); err != nil || proto != protoIKE {
		t.Fatalf("%d %v", proto, err)
	}
}
