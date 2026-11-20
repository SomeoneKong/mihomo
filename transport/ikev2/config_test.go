package ikev2

import (
	"bytes"
	"testing"
)

func TestParseSuite(t *testing.T) {
	s, err := parseSuite(" AES256-sha256-ecp384 ")
	if err != nil {
		t.Fatal(err)
	}
	if s.encrKeyLen != 32 || s.integ.name != "sha256" || s.dh.id != 20 || s.String() != "aes256-sha256-ecp384" {
		t.Fatalf("got %s", s)
	}
	if s, err := parseSuite("aes128-sha1"); err != nil || s.dh != nil {
		t.Fatalf("esp suite: %v %v", s, err)
	}
	for _, bad := range []string{"", "aes256", "sha256-ecp384", "aes256-sha256-ecp384-modp2048", "aes256gcm16-sha256", "aes256-md5"} {
		if _, err := parseSuite(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestPrepare(t *testing.T) {
	c := &Config{PSK: "psk", Username: "u", Password: "p"}
	if err := c.Prepare(); err != nil {
		t.Fatal(err)
	}
	if c.Port != 500 || c.IKELifetime != DefaultIKELifetime || len(c.ikeSuites) != 2 || len(c.espSuites) != 2 || c.espSuites[0].dh == nil {
		t.Fatalf("defaults not applied: %+v", c)
	}
	for _, bad := range []*Config{
		{},
		{PSK: "psk", Username: "u"},
		{PSK: "psk", IKEProposals: []string{"aes256-sha256"}},
	} {
		if err := bad.Prepare(); err == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
}

func TestDHAgreement(t *testing.T) {
	for _, g := range dhGroups {
		a, err := g.generate()
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.generate()
		if err != nil {
			t.Fatal(err)
		}
		s1, err := a.shared(b.pub)
		if err != nil {
			t.Fatal(err)
		}
		s2, err := b.shared(a.pub)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(s1, s2) {
			t.Errorf("%s: shared secrets differ", g.name)
		}
		if _, err := a.shared(make([]byte, len(a.pub))); err == nil {
			t.Errorf("%s: invalid public value accepted", g.name)
		}
	}
}
