package process

import (
	"net"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindProcessNameIgnoresForeignSource(t *testing.T) {
	// A UDP socket bound to the wildcard address matches a lookup by its port
	// alone, as an unconnected socket may send from any local address.
	pc, err := net.ListenPacket("udp4", "0.0.0.0:0")
	require.NoError(t, err)
	defer pc.Close()
	port := pc.LocalAddr().(*net.UDPAddr).Port

	_, path, err := FindProcessName(UDP, netip.MustParseAddr("127.0.0.1"), port)
	if err != nil {
		t.Skipf("process lookup is unavailable: %v", err)
	}
	require.NotEmpty(t, path)

	_, _, err = FindProcessName(UDP, netip.MustParseAddr("203.0.113.1"), port)
	require.ErrorIs(t, err, ErrNotFound)
}
