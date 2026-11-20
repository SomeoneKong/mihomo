package ikev2

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestMD4Vectors(t *testing.T) {
	// RFC 1320 appendix A.5.
	for input, want := range map[string]string{
		"":                           "31d6cfe0d16ae931b73c59d7e0c089c0",
		"a":                          "bde52cb31de33e46245e05fbdbd6fb24",
		"abc":                        "a448017aaf21d8525fc10ae87aa6729d",
		"message digest":             "d9130a8164549fe818874806e1c7014b",
		"abcdefghijklmnopqrstuvwxyz": "d79e1c308aa5bbcdeea8ed63df412da9",
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789": "043f8582f241db351ce627e153e7f0e4",
		strings.Repeat("1234567890", 8):                                  "e33b4ddc9c38f2199c3e7b164fcc0536",
	} {
		got := md4Sum([]byte(input))
		if hex.EncodeToString(got[:]) != want {
			t.Errorf("md4(%q) = %x, want %s", input, got, want)
		}
	}
}

// RFC 2759 section 9.2 and RFC 3079 section 3.5.3.
func TestMSCHAPv2Vectors(t *testing.T) {
	authChallenge := mustHex(t, "5B 5D 7C 7D 7B 3F 2F 3E 3C 2C 60 21 32 26 26 28")
	peerChallenge := mustHex(t, "21 40 23 24 25 5E 26 2A 28 29 5F 2B 3A 33 7C 7E")

	if got := challengeHash(peerChallenge, authChallenge, "User"); !bytes.Equal(got, mustHex(t, "D0 2E 43 86 BC E9 12 26")) {
		t.Errorf("challenge hash = %X", got)
	}
	passwordHash := ntPasswordHash("clientPass")
	if !bytes.Equal(passwordHash, mustHex(t, "44 EB BA 8D 53 12 B8 D6 11 47 44 11 F5 69 89 AE")) {
		t.Errorf("password hash = %X", passwordHash)
	}
	ntResponse := generateNTResponse(authChallenge, peerChallenge, "User", "clientPass")
	if !bytes.Equal(ntResponse, mustHex(t, "82 30 9E CD 8D 70 8B 5E A0 8F AA 39 81 CD 83 54 42 33 11 4A 3D 85 D6 DF")) {
		t.Errorf("NT response = %X", ntResponse)
	}
	if got := authenticatorResponse("clientPass", ntResponse, peerChallenge, authChallenge, "User"); got != "S=407A5589115FD0D6209F510FE9C04566932CDA56" {
		t.Errorf("authenticator response = %s", got)
	}
	hashHash := md4Sum(passwordHash)
	if !bytes.Equal(hashHash[:], mustHex(t, "41 C0 0C 58 4B D2 D9 1C 40 17 A2 A1 2F A5 9F 3F")) {
		t.Errorf("password hash hash = %X", hashHash)
	}
	master := mppeMasterKey(hashHash[:], ntResponse)
	if !bytes.Equal(master, mustHex(t, "FD EC E3 71 7A 8C 83 8C B3 88 E5 27 AE 3C DD 31")) {
		t.Errorf("master key = %X", master)
	}
	// The RFC 3079 sample send key is the server's, derived with Magic3; it is the second half of the MSK.
	if got := mppeStartKey(master, mppeMagic3); !bytes.Equal(got, mustHex(t, "8B 7C DC 14 9B 99 3A 1B A1 18 CB 15 3F 56 DC CB")) {
		t.Errorf("send start key = %X", got)
	}
	msk := mschapv2MSK("clientPass", ntResponse)
	if len(msk) != 32 || !bytes.Equal(msk[16:], mustHex(t, "8B 7C DC 14 9B 99 3A 1B A1 18 CB 15 3F 56 DC CB")) {
		t.Errorf("msk = %X", msk)
	}
}

func TestEAPPeerFlow(t *testing.T) {
	peer := &eapPeer{username: "user1", password: "pass1"}

	resp, err := peer.respond(eapPacket(eapCodeRequest, 7, eapTypeIdentity, nil))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(resp, eapPacket(eapCodeResponse, 7, eapTypeIdentity, []byte("user1"))) {
		t.Fatalf("identity response = %x", resp)
	}

	// A method other than MSCHAPv2 is refused with a Nak proposing MSCHAPv2.
	resp, err = peer.respond(eapPacket(eapCodeRequest, 8, 4, []byte{0}))
	if err != nil || !bytes.Equal(resp, eapPacket(eapCodeResponse, 8, eapTypeNak, []byte{eapTypeMSCHAPv2})) {
		t.Fatalf("nak = %x, %v", resp, err)
	}

	authChallenge := bytes.Repeat([]byte{0x42}, 16)
	challenge := append([]byte{mschapChallenge, 3, 0, 0, 16}, authChallenge...)
	challenge = append(challenge, "gw"...)
	binary.BigEndian.PutUint16(challenge[2:], uint16(len(challenge)))
	resp, err = peer.respond(eapPacket(eapCodeRequest, 9, eapTypeMSCHAPv2, challenge))
	if err != nil {
		t.Fatal(err)
	}
	data := resp[5:]
	if data[0] != mschapResponse || data[1] != 3 || int(binary.BigEndian.Uint16(data[2:])) != len(data) || data[4] != 49 {
		t.Fatalf("bad MSCHAPv2 response header %x", data[:5])
	}
	peerChallenge, ntResponse := data[5:21], data[29:53]
	if !bytes.Equal(ntResponse, generateNTResponse(authChallenge, peerChallenge, "user1", "pass1")) {
		t.Fatal("NT response does not match the challenge")
	}
	if string(data[54:]) != "user1" {
		t.Fatalf("name = %q", data[54:])
	}

	bad := append([]byte{mschapSuccess, 3, 0, 0}, "S=0000000000000000000000000000000000000000 M=ok"...)
	if _, err := (&eapPeer{username: "user1", password: "pass1", ntResponse: peer.ntResponse, authResponse: peer.authResponse}).
		respond(eapPacket(eapCodeRequest, 10, eapTypeMSCHAPv2, bad)); err == nil {
		t.Fatal("a wrong authenticator response was accepted")
	}

	success := append([]byte{mschapSuccess, 3, 0, 0}, authenticatorResponse("pass1", ntResponse, peerChallenge, authChallenge, "user1")+" M=Welcome"...)
	resp, err = peer.respond(eapPacket(eapCodeRequest, 10, eapTypeMSCHAPv2, success))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(resp, eapPacket(eapCodeResponse, 10, eapTypeMSCHAPv2, []byte{mschapSuccess})) {
		t.Fatalf("success response = %x", resp)
	}
	if !bytes.Equal(peer.msk, mschapv2MSK("pass1", ntResponse)) {
		t.Fatal("msk not derived")
	}

	failure := append([]byte{mschapFailure, 4, 0, 0}, "E=691 R=0 V=3 M=bad password"...)
	if _, err := peer.respond(eapPacket(eapCodeRequest, 11, eapTypeMSCHAPv2, failure)); err == nil || !strings.Contains(err.Error(), "E=691") {
		t.Fatalf("failure not reported: %v", err)
	}
}
