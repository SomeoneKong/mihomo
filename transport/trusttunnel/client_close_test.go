package trusttunnel

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/metacubex/http"
	"github.com/metacubex/sing/common/logger"
	M "github.com/metacubex/sing/common/metadata"
	"github.com/metacubex/sing/common/network"
)

type closeTestHandler func(net.Conn)

func (h closeTestHandler) NewConnection(ctx context.Context, conn net.Conn, metadata M.Metadata) error {
	h(conn)
	return nil
}

func (h closeTestHandler) NewPacketConnection(ctx context.Context, conn network.PacketConn, metadata M.Metadata) error {
	return conn.Close()
}

func newCloseTestClient(t *testing.T, handle func(net.Conn)) *Client {
	service := NewService(ServiceOptions{Ctx: context.Background(), Logger: logger.NOP(), Handler: closeTestHandler(handle)})
	service.UpdateUsers(map[string]string{"user": "password"})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(ln, nil, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })

	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	client := &Client{
		ctx:    context.Background(),
		server: ln.Addr().String(),
		auth:   buildAuth("user", "password"),
		roundTripper: &http.Transport{
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "tcp", ln.Addr().String())
			},
			Protocols: protocols,
		},
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// What was written before Close reaches the server, followed by the end of
// the upload.
func TestClientCloseDeliversUpload(t *testing.T) {
	received := make(chan int64, 1)
	client := newCloseTestClient(t, func(conn net.Conn) {
		n, _ := io.Copy(io.Discard, conn)
		_ = conn.Close()
		received <- n
	})
	for _, size := range []int{32 << 10, 4 << 20} {
		conn, err := client.Dial(context.Background(), "example.com:443")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write(make([]byte, size)); err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		select {
		case n := <-received:
			if n != int64(size) {
				t.Errorf("server got %d of the %d bytes written before Close", n, size)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("server did not see the end of a %d-byte upload", size)
		}
	}
}

// Close does not keep the download going: the server's writes fail soon
// after it, though the server would send for ever.
func TestClientCloseStopsDownload(t *testing.T) {
	stopped := make(chan struct{})
	client := newCloseTestClient(t, func(conn net.Conn) {
		defer close(stopped)
		defer conn.Close()
		go func() { _, _ = io.Copy(io.Discard, conn) }()
		chunk := make([]byte, 32<<10)
		for {
			if _, err := conn.Write(chunk); err != nil {
				return
			}
		}
	})
	conn, err := client.Dial(context.Background(), "example.com:443")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("request")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(conn, make([]byte, 1<<20)); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Error("read after Close succeeded")
	}
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("server still sending 2s after Close")
	}
}
