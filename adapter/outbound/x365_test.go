package outbound

import (
	"io"
	"net"
	"net/netip"
	"testing"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/transport/x365"

	"github.com/gofrs/uuid/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewX365Defaults(t *testing.T) {
	option := X365Option{
		Name:       "x365",
		Server:     "127.0.0.1",
		Port:       443,
		UUID:       "c5f1a574-20a8-4f06-8880-2ae7a4af2f7e",
		ServerName: "example.com",
		XHTTPOpts:  XHTTPOptions{Path: "/"},
	}

	t.Run("default", func(t *testing.T) {
		proxy, err := NewX365(option)
		require.NoError(t, err)
		defer proxy.Close()

		assert.Equal(t, x365.UserAgent, proxy.option.XHTTPOpts.Headers["User-Agent"])
		assert.Equal(t, "stream-one", proxy.option.XHTTPOpts.Mode)
		assert.False(t, proxy.SupportUDP())
		assert.False(t, proxy.SupportUOT())
	})

	t.Run("custom user agent", func(t *testing.T) {
		custom := option
		custom.XHTTPOpts.Headers = map[string]string{"user-agent": "custom"}
		proxy, err := NewX365(custom)
		require.NoError(t, err)
		defer proxy.Close()

		assert.Equal(t, map[string]string{"user-agent": "custom"}, proxy.option.XHTTPOpts.Headers)
	})

	t.Run("mode", func(t *testing.T) {
		for _, mode := range []string{"auto", "stream-one"} {
			custom := option
			custom.XHTTPOpts.Mode = mode
			proxy, err := NewX365(custom)
			require.NoError(t, err, mode)
			assert.Equal(t, "stream-one", proxy.option.XHTTPOpts.Mode, mode)
			_ = proxy.Close()
		}
		for _, mode := range []string{"stream-up", "packet-up"} {
			custom := option
			custom.XHTTPOpts.Mode = mode
			_, err := NewX365(custom)
			assert.Error(t, err, mode)
		}

		custom := option
		custom.XHTTPOpts.DownloadSettings = &XHTTPDownloadSettings{}
		_, err := NewX365(custom)
		assert.Error(t, err, "download-settings needs a separate download request, which x365 does not have")
	})
}

func TestX365PacketConn(t *testing.T) {
	const id = "c5f1a574-20a8-4f06-8880-2ae7a4af2f7e"
	proxy, err := NewX365(X365Option{
		Name:       "x365",
		Server:     "127.0.0.1",
		Port:       443,
		UUID:       id,
		UDP:        true,
		ServerName: "example.com",
		XHTTPOpts:  XHTTPOptions{Path: "/"},
	})
	require.NoError(t, err)
	defer proxy.Close()
	assert.True(t, proxy.SupportUDP())
	assert.False(t, proxy.ProxyInfo().XUDP)

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	metadata := &C.Metadata{NetWork: C.UDP, DstIP: netip.MustParseAddr("1.1.1.1"), DstPort: 53}
	conn := proxy.packetConn(client, metadata)

	go func() {
		_, _ = conn.WriteTo([]byte("query"), metadata.UDPAddr())
	}()
	want := []byte("X365")
	want = append(want, 0x01, 0x02) // version, UDP
	want = append(want, uuid.FromStringOrNil(id).Bytes()...)
	want = append(want, 0, 53, x365.AtypIPv4, 1, 1, 1, 1)
	want = append(want, 0, 5)
	want = append(want, "query"...)
	got := make([]byte, len(want))
	_, err = io.ReadFull(server, got)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	go func() {
		_, _ = server.Write([]byte("X365\x00\x00\x06answer"))
	}()
	packet := make([]byte, 64)
	n, addr, err := conn.ReadFrom(packet)
	require.NoError(t, err)
	assert.Equal(t, "answer", string(packet[:n]))
	assert.Equal(t, metadata.UDPAddr().String(), addr.String())
}
