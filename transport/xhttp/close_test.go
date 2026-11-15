package xhttp

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/metacubex/http"
)

func startCloseTestServer(t *testing.T, mode string, handle func(net.Conn)) string {
	handler, err := NewServerHandler(ServerOption{
		Config:      Config{Host: "example.com", Path: "/xhttp", Mode: mode},
		ConnHandler: handle,
	})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{Handler: handler, Protocols: protocols}
	go server.Serve(ln)
	t.Cleanup(func() { _ = server.Close() })
	return ln.Addr().String()
}

func newCloseTestClient(t *testing.T, addr, mode string) *Client {
	makeTransport := func() http.RoundTripper {
		return NewTransport(func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		}, func(ctx context.Context, conn net.Conn, isH2 bool) (net.Conn, error) {
			return conn, nil
		}, nil, nil, 0)
	}
	client, err := NewClient(&Config{Host: "example.com", Path: "/xhttp", Mode: mode}, makeTransport, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// What was written before Close reaches the server, followed by the end of
// the upload.
func TestClientCloseDeliversUpload(t *testing.T) {
	for _, mode := range []string{"stream-one", "stream-up", "packet-up"} {
		t.Run(mode, func(t *testing.T) {
			received := make(chan int64, 1)
			addr := startCloseTestServer(t, mode, func(conn net.Conn) {
				n, _ := io.Copy(io.Discard, conn)
				_ = conn.Close()
				received <- n
			})
			client := newCloseTestClient(t, addr, mode)
			for _, size := range []int{32 << 10, 4 << 20} {
				conn, err := client.Dial(context.Background())
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
		})
	}
}

// Close does not keep the download going: the server's writes fail soon
// after it, though the server would send for ever.
func TestClientCloseStopsDownload(t *testing.T) {
	for _, mode := range []string{"stream-one", "stream-up"} {
		t.Run(mode, func(t *testing.T) {
			stopped := make(chan struct{})
			addr := startCloseTestServer(t, mode, func(conn net.Conn) {
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
			client := newCloseTestClient(t, addr, mode)
			conn, err := client.Dial(context.Background())
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
		})
	}
}
