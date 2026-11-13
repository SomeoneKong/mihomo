package ninja

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type memoryConn struct {
	input      io.Reader
	output     bytes.Buffer
	shortWrite int
	writeError error
}

func (c *memoryConn) Read(b []byte) (int, error) { return c.input.Read(b) }
func (c *memoryConn) Write(b []byte) (int, error) {
	if c.writeError != nil {
		return 0, c.writeError
	}
	if c.shortWrite > 0 && len(b) > c.shortWrite {
		b = b[:c.shortWrite]
	}
	return c.output.Write(b)
}
func (*memoryConn) Close() error                     { return nil }
func (*memoryConn) LocalAddr() net.Addr              { return nil }
func (*memoryConn) RemoteAddr() net.Addr             { return nil }
func (*memoryConn) SetDeadline(time.Time) error      { return nil }
func (*memoryConn) SetReadDeadline(time.Time) error  { return nil }
func (*memoryConn) SetWriteDeadline(time.Time) error { return nil }

func testClient(t *testing.T) *Client {
	t.Helper()
	c, e := NewClient(Options{Method: "aes-128-gcm", Password: "test-user", NodePassword: "test-node"})
	if e != nil {
		t.Fatal(e)
	}
	return c
}

func TestPassKnownFrame(t *testing.T) {
	// Generated independently with Python hashlib SHA3/SHAKE and cryptography AESGCM.
	wire, _ := hex.DecodeString("000102030405060708090a0bb1893ebf4a4371c37021fefb1be597a30d3f8c314b6f58b39a23ea2359e1ffcdb7daf0528e7a72be92f66b10")
	for _, corrupt := range []bool{false, true} {
		packet := append([]byte(nil), wire...)
		if corrupt {
			packet[len(packet)-1] ^= 1
		}
		c := newPass(&memoryConn{input: bytes.NewReader(packet)}, Options{PassOpts: PassOptions{Password: "test-pass"}})
		out := make([]byte, 32)
		n, e := c.Read(out)
		if corrupt {
			if e == nil || n != 0 {
				t.Fatal("authenticated corruption yielded plaintext")
			}
			continue
		}
		if e != nil || string(out[:n]) != "hello" {
			t.Fatalf("known frame: %q %v", out[:n], e)
		}
	}
}

func TestLargeFirstWriteAndShortUnderlyingWrites(t *testing.T) {
	client := testClient(t)
	raw := &memoryConn{input: bytes.NewReader(nil), shortWrite: 7}
	addr := []byte{1, 192, 0, 2, 1, 0, 80}
	c := client.newConn(raw, addr, 1)
	input := bytes.Repeat([]byte("large-write"), 15000)
	if n, e := c.Write(input); e != nil || n != len(input) {
		t.Fatalf("write: %d %v", n, e)
	}
	r := bytes.NewReader(raw.output.Bytes())
	salt, _ := readN(r, 16)
	readN(r, 16)
	key := derive(subkeyContext, append(derive(pskContext, []byte("test-user")), salt...))
	a := aead(key)
	nonce := make([]byte, 12)
	open := func(size int) []byte {
		t.Helper()
		b, e := readN(r, size+16)
		if e != nil {
			t.Fatal(e)
		}
		b, e = a.Open(nil, nonce, b, nil)
		if e != nil {
			t.Fatal(e)
		}
		increment(nonce)
		return b
	}
	head := open(12)
	first := open(int(binary.BigEndian.Uint16(head[10:])))
	pad := int(binary.BigEndian.Uint16(first[len(addr):]))
	decoded := append([]byte(nil), first[len(addr)+2+pad:]...)
	for r.Len() > 0 {
		h := open(4)
		size := int(binary.BigEndian.Uint16(h))
		padding := int(binary.BigEndian.Uint16(h[2:]))
		p := open(size)
		if padding > len(p) {
			t.Fatal("invalid padding")
		}
		decoded = append(decoded, p[:len(p)-padding]...)
	}
	if !bytes.Equal(decoded, input) {
		t.Fatalf("data lost across first/multiple frames: %d of %d", len(decoded), len(input))
	}
}

func responseBytes(c *ninjaConn, payloads ...[]byte) []byte {
	salt := bytes.Repeat([]byte{0x22}, 16)
	a := aead(derive(subkeyContext, append(append([]byte(nil), c.passPSK...), salt...)))
	nonce := make([]byte, 12)
	out := append([]byte(nil), salt...)
	for i, p := range payloads {
		var header []byte
		if i == 0 {
			header = make([]byte, 29)
			header[0] = 1
			copy(header[9:25], c.salt)
			binary.BigEndian.PutUint16(header[25:], uint16(len(p)))
		} else {
			header = binary.BigEndian.AppendUint16(nil, uint16(len(p)))
			header = binary.BigEndian.AppendUint16(header, 0)
		}
		out = append(out, a.Seal(nil, nonce, header, nil)...)
		increment(nonce)
		out = append(out, a.Seal(nil, nonce, p, nil)...)
		increment(nonce)
	}
	return out
}

// passFrames carries data as a server sends it in pass frames of at most size bytes.
func passFrames(data []byte, size int) []byte {
	if len(data) == 0 {
		return nil
	}
	raw := &memoryConn{}
	w := newPass(raw, Options{PassOpts: PassOptions{Password: "test-pass"}})
	w.first = true // only the client sends the verification prefix
	for len(data) > 0 {
		n := minInt(len(data), size)
		w.Write(data[:n])
		data = data[n:]
	}
	return raw.output.Bytes()
}

func TestCutResponseIsNotACleanEnd(t *testing.T) {
	for _, pass := range []bool{false, true} {
		client, err := NewClient(Options{Method: "aes-128-gcm", Password: "test-user", NodePassword: "test-node", Pass: pass, PassOpts: PassOptions{Password: "test-pass"}})
		if err != nil {
			t.Fatal(err)
		}
		salt := bytes.Repeat([]byte{0x11}, 16)
		read := func(wire []byte) ([]byte, error) {
			c := client.newConn(&memoryConn{input: bytes.NewReader(wire)}, []byte{1, 192, 0, 2, 1, 0, 80}, 1)
			c.salt = salt
			return io.ReadAll(c)
		}
		probe := client.newConn(&memoryConn{}, nil, 1)
		probe.salt = salt
		response := responseBytes(probe, []byte("test"), []byte("more"))
		// The two frames end at 81 and 121 bytes; every other cut is inside one.
		clean := map[int]string{0: "", 81: "test", 121: "testmore"}
		for _, cut := range []int{0, 16, 40, 61, 70, 81, 90, 101, 110, 121} {
			wire := response[:cut]
			if pass {
				wire = passFrames(wire, 25)
			}
			data, err := read(wire)
			if want, ok := clean[cut]; ok {
				if err != nil || string(data) != want {
					t.Fatalf("pass=%v cut=%d: %q %v", pass, cut, data, err)
				}
			} else if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("pass=%v cut=%d ended with %v", pass, cut, err)
			}
		}
		if pass {
			// After the IV, inside and after the header, and inside the payload of a pass frame.
			wire := passFrames(response, 25)
			for _, cut := range []int{12, 22, 32, 40} {
				if _, err := read(wire[:cut]); !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("pass frame cut=%d ended with %v", cut, err)
				}
			}
		}
	}
}

func TestDatagramTruncationPreservesNextPacket(t *testing.T) {
	raw := &memoryConn{}
	c := testClient(t).newConn(raw, []byte{1, 1, 1, 1, 1, 0, 53}, 2)
	c.salt = bytes.Repeat([]byte{0x11}, 16)
	first := append([]byte{1, 1, 1, 1, 1, 0, 53}, bytes.Repeat([]byte{0x61}, 100)...)
	second := append([]byte{1, 8, 8, 8, 8, 0, 53}, []byte("second")...)
	raw.input = bytes.NewReader(responseBytes(c, first, second))
	pc := &packetConn{c}
	output := make([]byte, 10)
	n, addr, e := pc.ReadFrom(output)
	if e != nil || n != 10 || addr.String() != "1.1.1.1:53" {
		t.Fatalf("first packet: %d %v %v", n, addr, e)
	}
	n, addr, e = pc.ReadFrom(output)
	if e != nil || string(output[:n]) != "second" || addr.String() != "8.8.8.8:53" {
		t.Fatalf("second packet: %q %v %v", output[:n], addr, e)
	}
}

func TestResponseRejectsWrongEchoedSalt(t *testing.T) {
	raw := &memoryConn{}
	c := testClient(t).newConn(raw, []byte{1, 1, 1, 1, 1, 0, 53}, 1)
	c.salt = bytes.Repeat([]byte{0x11}, 16)
	wire := responseBytes(c, []byte("test"))
	c.salt = bytes.Repeat([]byte{0x33}, 16)
	raw.input = bytes.NewReader(wire)
	if n, e := c.Read(make([]byte, 16)); n != 0 || e == nil {
		t.Fatal("response accepted a salt from another connection")
	}
}

func TestConcurrentReadAndWrite(t *testing.T) {
	raw := &memoryConn{}
	c := testClient(t).newConn(raw, []byte{1, 192, 0, 2, 1, 0, 80}, 1)
	raw.input = bytes.NewReader(responseBytes(c, []byte("response")))
	start := make(chan struct{})
	errors := make(chan error, 2)
	var done sync.WaitGroup
	done.Add(2)
	go func() {
		defer done.Done()
		<-start
		_, err := c.Write(bytes.Repeat([]byte("request"), 10000))
		errors <- err
	}()
	go func() { defer done.Done(); <-start; _, err := c.Read(make([]byte, 128)); errors <- err }()
	close(start)
	done.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestFailedWriteDoesNotReportUnsentPlaintext(t *testing.T) {
	for _, pass := range []bool{false, true} {
		client, err := NewClient(Options{Method: "aes-128-gcm", Password: "test-user", NodePassword: "test-node", Pass: pass, PassOpts: PassOptions{Password: "test-pass"}})
		if err != nil {
			t.Fatal(err)
		}
		raw := &memoryConn{input: bytes.NewReader(nil)}
		conn, err := client.StreamConn(raw, []byte{1, 192, 0, 2, 1, 0, 80})
		if err != nil {
			t.Fatal(err)
		}
		raw.writeError = io.ErrClosedPipe
		if n, err := conn.Write([]byte("request")); n != 0 || err == nil {
			t.Fatalf("pass=%v failed write: %d %v", pass, n, err)
		}
	}
}

func TestDatagramSourcesAreUDPAddrs(t *testing.T) {
	raw := &memoryConn{}
	c := testClient(t).newConn(raw, []byte{1, 1, 1, 1, 1, 0, 53}, 2)
	c.salt = bytes.Repeat([]byte{0x11}, 16)
	v6 := append([]byte{4}, net.ParseIP("2001:db8::1").To16()...)
	v6 = append(v6, 0x01, 0xbb)
	raw.input = bytes.NewReader(responseBytes(c,
		append([]byte{1, 1, 1, 1, 1, 0, 53}, 'a'),
		append(v6, 'b'),
	))
	pc := &packetConn{c}
	for _, want := range []string{"1.1.1.1:53", "[2001:db8::1]:443"} {
		_, addr, err := pc.ReadFrom(make([]byte, 16))
		if err != nil {
			t.Fatal(err)
		}
		udp, ok := addr.(*net.UDPAddr)
		if !ok || udp.String() != want {
			t.Fatalf("source %T %v, want *net.UDPAddr %s", addr, addr, want)
		}
	}
}

func TestFailedDirectionStaysFailed(t *testing.T) {
	for _, pass := range []bool{false, true} {
		client, err := NewClient(Options{Method: "aes-128-gcm", Password: "test-user", NodePassword: "test-node", Pass: pass, PassOpts: PassOptions{Password: "test-pass"}})
		if err != nil {
			t.Fatal(err)
		}
		raw := &memoryConn{input: bytes.NewReader(nil)}
		conn, err := client.StreamConn(raw, []byte{1, 192, 0, 2, 1, 0, 80})
		if err != nil {
			t.Fatal(err)
		}
		raw.writeError = io.ErrClosedPipe
		if _, err := conn.Write([]byte("lost")); err == nil {
			t.Fatalf("pass=%v: write succeeded", pass)
		}
		raw.writeError = nil
		written := raw.output.Len()
		if _, err := conn.Write([]byte("after")); err == nil || raw.output.Len() != written {
			t.Fatalf("pass=%v: wrote again after a failed write: %v", pass, err)
		}
	}

	raw := &memoryConn{}
	c := testClient(t).newConn(raw, []byte{1, 1, 1, 1, 1, 0, 53}, 1)
	c.salt = bytes.Repeat([]byte{0x11}, 16)
	wire := responseBytes(c, []byte("test"))
	c.salt = bytes.Repeat([]byte{0x33}, 16)
	reader := bytes.NewReader(append(wire, wire...))
	raw.input = reader
	if _, err := c.Read(make([]byte, 16)); err == nil {
		t.Fatal("accepted a wrong salt")
	}
	left := reader.Len()
	if _, err := c.Read(make([]byte, 16)); err == nil || reader.Len() != left {
		t.Fatal("read on after the response was rejected")
	}
}
