package ikev2

import (
	"bytes"
	"testing"
)

func newTestESPPair(t *testing.T, proposal string) (sender, receiver *espSA) {
	t.Helper()
	s, err := parseSuite(proposal)
	if err != nil {
		t.Fatal(err)
	}
	encKey, integKey := randomBytes(s.encrKeyLen), randomBytes(s.integ.size)
	if sender, err = newESPSA(0x1234, s, encKey, integKey); err != nil {
		t.Fatal(err)
	}
	if receiver, err = newESPSA(0x1234, s, encKey, integKey); err != nil {
		t.Fatal(err)
	}
	return sender, receiver
}

func TestESPRoundTrip(t *testing.T) {
	for _, p := range []string{"aes256-sha256", "aes128-sha1", "aes192-sha512"} {
		sender, receiver := newTestESPPair(t, p)
		for size := 1; size <= 70; size++ {
			packet := bytes.Repeat([]byte{0x45}, size)
			b, err := sender.seal(packet)
			if err != nil {
				t.Fatal(err)
			}
			got, err := receiver.open(b)
			if err != nil {
				t.Fatalf("%s size %d: %v", p, size, err)
			}
			if !bytes.Equal(got, packet) {
				t.Fatalf("%s size %d: got %x", p, size, got)
			}
		}
	}
}

func TestESPRejects(t *testing.T) {
	sender, receiver := newTestESPPair(t, "aes256-sha256")
	b, _ := sender.seal([]byte{0x45, 1, 2, 3})
	if _, err := receiver.open(b); err != nil {
		t.Fatal(err)
	}
	if _, err := receiver.open(b); err != errReplay {
		t.Fatalf("replay: %v", err)
	}
	b, _ = sender.seal([]byte{0x45, 1, 2, 3})
	b[20] ^= 1
	if _, err := receiver.open(b); err == nil {
		t.Fatal("tampered packet accepted")
	}
}

func TestReplayWindow(t *testing.T) {
	sa := &espSA{}
	for _, c := range []struct {
		seq uint32
		ok  bool
	}{{1, true}, {3, true}, {2, true}, {2, false}, {100, true}, {36, false}, {37, true}, {37, false}, {0, false}} {
		if got := sa.checkReplay(c.seq); got != c.ok {
			t.Errorf("seq %d: got %v, want %v", c.seq, got, c.ok)
		}
	}
}
