package http

import (
	"io"
	"net"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
	authStore "github.com/metacubex/mihomo/listener/auth"

	"github.com/stretchr/testify/require"
)

type dialTunnel struct {
	C.Tunnel
	dials chan *C.Metadata
}

func (t *dialTunnel) HandleTCPConn(conn net.Conn, metadata *C.Metadata) {
	conn.Close()
	t.dials <- metadata
}

func TestAbsoluteURIDecidesTheTarget(t *testing.T) {
	tunnel := &dialTunnel{dials: make(chan *C.Metadata, 1)}
	client, server := net.Pipe()
	defer client.Close()
	go HandleConn(server, tunnel, authStore.Nil)
	go io.Copy(io.Discard, client)

	_, err := io.WriteString(client, "GET http://target.example/ HTTP/1.1\r\nHost: other.example\r\n\r\n")
	require.NoError(t, err)
	select {
	case metadata := <-tunnel.dials:
		require.Equal(t, "target.example", metadata.Host)
	case <-time.After(5 * time.Second):
		t.Fatal("the request was not forwarded")
	}
}
