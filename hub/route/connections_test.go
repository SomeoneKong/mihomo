package route

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/textproto"
	"strings"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeConn implements only what a tracker reads when it is created.
type fakeConn struct {
	C.Conn
	chain C.Chain
}

func (c *fakeConn) Chains() C.Chain { return c.chain }

func (c *fakeConn) ProviderChains() C.Chain { return nil }

func (c *fakeConn) RemoteDestination() string { return "" }

func TestConnectionsLeaveOutUnderlying(t *testing.T) {
	manager := statistic.DefaultManager
	carried := statistic.NewTCPTracker(&fakeConn{chain: C.Chain{"hk", "group"}}, manager, &C.Metadata{}, nil, 0, 0, true,
		statistic.WithDialerChains([]C.Chain{{"relay-node", "relay"}}))
	underlying := statistic.NewTCPTracker(&fakeConn{chain: C.Chain{"relay-node", "relay"}}, manager, &C.Metadata{Type: C.INNER}, nil, 0, 0, false,
		statistic.AsUnderlying(context.Background()))
	t.Cleanup(func() {
		manager.Leave(carried)
		manager.Leave(underlying)
	})

	ids := func(query string) map[string][]C.Chain {
		recorder := httptest.NewRecorder()
		getConnections(recorder, httptest.NewRequest("GET", "/connections"+query, nil))
		var snapshot struct {
			Connections []struct {
				ID           string    `json:"id"`
				DialerChains []C.Chain `json:"dialerChains"`
			} `json:"connections"`
		}
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &snapshot))
		found := map[string][]C.Chain{}
		for _, connection := range snapshot.Connections {
			found[connection.ID] = connection.DialerChains
		}
		return found
	}

	listed := ids("")
	assert.Contains(t, listed, carried.ID())
	assert.NotContains(t, listed, underlying.ID())
	assert.Equal(t, []C.Chain{{"relay-node", "relay"}}, listed[carried.ID()])

	all := ids("?underlying=true")
	assert.Contains(t, all, carried.ID())
	assert.Contains(t, all, underlying.ID())
	assert.Nil(t, all[underlying.ID()])
}

func dialConnectionsWebSocket(t *testing.T, serverURL, interval string) (net.Conn, *bufio.Reader, string) {
	t.Helper()
	conn, err := net.Dial("tcp", strings.TrimPrefix(serverURL, "http://"))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	_, err = fmt.Fprintf(conn, "GET /connections?interval=%s HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n", interval)
	require.NoError(t, err)
	reader := bufio.NewReader(conn)
	status, err := reader.ReadString('\n')
	require.NoError(t, err)
	return conn, reader, status
}

func TestConnectionsWebSocketRejectsInvalidInterval(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(getConnections))
	defer server.Close()
	for _, interval := range []string{"0", "-1", "abc", "9223372036855"} {
		_, _, status := dialConnectionsWebSocket(t, server.URL, interval)
		assert.Contains(t, status, " 400 ", "interval=%s", interval)
	}
}

func TestConnectionsWebSocketEndsWhenClientLeaves(t *testing.T) {
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		getConnections(w, r)
	}))
	defer server.Close()

	conn, reader, status := dialConnectionsWebSocket(t, server.URL, "3600000")
	require.Contains(t, status, " 101 ")
	// Read everything sent so far, so that closing ends the stream cleanly
	// instead of resetting it, which would fail the next write right away.
	_, err := textproto.NewReader(reader).ReadMIMEHeader()
	require.NoError(t, err)
	messages, err := wsutil.ReadServerMessage(reader, nil)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, ws.OpText, messages[0].OpCode)
	conn.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the push to a client that left goes on until its next write")
	}
}
