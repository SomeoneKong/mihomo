package http

import (
	"bufio"
	"io"
	"net"
	"testing"
	"time"

	authStore "github.com/metacubex/mihomo/listener/auth"

	"github.com/metacubex/http"
	"github.com/stretchr/testify/require"
)

func TestTheBodyOfARefusedRequestIsNotTheNextRequest(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	go HandleConn(server, nil, authStore.Nil)

	// not an absolute URL: refused, its body unread by any transport
	_, err := io.WriteString(client, "POST /relative HTTP/1.1\r\nHost: example.com\r\nProxy-Connection: keep-alive\r\n"+
		"Content-Length: 11\r\n\r\nhello world"+
		"GET /again HTTP/1.1\r\nHost: example.com\r\nProxy-Connection: keep-alive\r\n\r\n")
	require.NoError(t, err)
	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(client)
	for i := 0; i < 2; i++ {
		resp, err := http.ReadResponse(reader, nil)
		require.NoError(t, err, "response %d", i+1)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		_ = resp.Body.Close()
	}
}
