package protocol

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	N "github.com/metacubex/mihomo/common/net"
)

func newTestAuthAES128() *authAES128 {
	return newAuthAES128SHA1(&Base{Key: bytes.Repeat([]byte{1}, 16)}).(*authAES128)
}

// A read from the server that ends within a frame must not return without data.
func TestStreamReadWaitsForWholeFrame(t *testing.T) {
	a := newTestAuthAES128()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	conn := a.StreamConn(client, make([]byte, 16))

	// the server frames its data like the client after the auth header
	encoder := &authAES128{Base: a.Base, authAES128Function: a.authAES128Function, userData: a.userData, packID: 1, hasSentHeader: true}
	var frame bytes.Buffer
	if err := encoder.Encode(&frame, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = server.Write(frame.Bytes()[:3])
		_, _ = server.Write(frame.Bytes()[3:])
	}()

	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	b := make([]byte, 16)
	n, err := conn.Read(b)
	if err != nil {
		t.Fatal(err)
	}
	if string(b[:n]) != "hello" {
		t.Fatalf("got %q", b[:n])
	}
}

type packetsConn struct {
	N.EnhancePacketConn
	packets [][]byte
}

func (c *packetsConn) ReadFrom(b []byte) (int, net.Addr, error) {
	if len(c.packets) == 0 {
		return 0, nil, io.EOF
	}
	n := copy(b, c.packets[0])
	c.packets = c.packets[1:]
	return n, &net.UDPAddr{}, nil
}

func (c *packetsConn) WaitReadFrom() ([]byte, func(), net.Addr, error) {
	if len(c.packets) == 0 {
		return nil, nil, nil, io.EOF
	}
	data := c.packets[0]
	c.packets = c.packets[1:]
	return data, nil, &net.UDPAddr{}, nil
}

// A malformed packet is dropped; the packets after it still arrive.
func TestPacketReadDropsMalformedPacket(t *testing.T) {
	a := newTestAuthAES128()
	valid := append([]byte("hello"), a.hmac(a.Key, []byte("hello"))[:4]...)
	newConn := func() N.EnhancePacketConn {
		return a.PacketConn(&packetsConn{packets: [][]byte{[]byte("garbage-packet"), valid}})
	}

	b := make([]byte, 64)
	n, _, err := newConn().ReadFrom(b)
	if err != nil || string(b[:n]) != "hello" {
		t.Fatalf("ReadFrom got %q, %v", b[:n], err)
	}

	data, _, _, err := newConn().WaitReadFrom()
	if err != nil || string(data) != "hello" {
		t.Fatalf("WaitReadFrom got %q, %v", data, err)
	}
}
