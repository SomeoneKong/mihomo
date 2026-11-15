package gun

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/metacubex/http"
)

// A server that accepted the stream but never answers must not keep the conn
// alive after Close: pending reads return and the stream is cancelled.
func TestCloseEndsUnansweredStream(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	arrived := make(chan struct{})
	cancelled := make(chan struct{})
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{
		Protocols: protocols,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(arrived)
			<-r.Context().Done()
			close(cancelled)
		}),
	}
	go server.Serve(ln)
	defer server.Close()

	transport := NewTransport(func(ctx context.Context, network, addr string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", ln.Addr().String())
	}, nil, &Config{Host: "example.com"})
	defer transport.Close()

	conn, err := transport.Dial()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("stream never reached the server")
	}

	readErr := make(chan error, 1)
	go func() {
		_, err := conn.Read(make([]byte, 1))
		readErr <- err
	}()
	_ = conn.Close()

	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("read on a closed conn succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read still blocked after Close")
	}
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("stream still open on the server after Close")
	}
}
