//go:build with_gvisor && !no_tailscale

package outbound

import (
	"net/netip"
	"testing"

	"github.com/metacubex/tailscale/ipn"
	"github.com/metacubex/tailscale/tailcfg"

	"github.com/stretchr/testify/require"
)

func TestTailscaleExitNodeIsTriedAgainAsPeersChange(t *testing.T) {
	node := func(prefixes ...string) *tailcfg.Node {
		n := &tailcfg.Node{}
		for _, p := range prefixes {
			n.AllowedIPs = append(n.AllowedIPs, netip.MustParsePrefix(p))
		}
		return n
	}
	state := func(s ipn.State) *ipn.Notify { return &ipn.Notify{State: &s} }
	peers := func(nodes ...*tailcfg.Node) *ipn.Notify { return &ipn.Notify{PeersChanged: nodes} }
	peer := node("100.64.0.2/32")
	subnetRouter := node("100.64.0.3/32", "192.168.1.0/24")
	exitNode := node("100.64.0.1/32", "0.0.0.0/0", "::/0")

	var s exitNodeSetter
	require.False(t, s.due(state(ipn.Starting)))
	require.False(t, s.due(peers(exitNode)), "not running yet")
	require.True(t, s.due(state(ipn.Running)))
	require.False(t, s.due(peers(peer, subnetRouter)), "no peer offers exit routes")
	require.True(t, s.due(peers(peer, exitNode)))
	require.False(t, s.due(state(ipn.NeedsLogin)))
	require.False(t, s.due(peers(exitNode)), "no longer running")
}
