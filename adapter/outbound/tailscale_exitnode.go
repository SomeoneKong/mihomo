//go:build with_gvisor && !no_tailscale

package outbound

import (
	"context"
	"fmt"

	"github.com/metacubex/mihomo/log"

	"github.com/metacubex/tailscale/ipn"
	"github.com/metacubex/tailscale/net/tsaddr"
	"github.com/metacubex/tailscale/tailcfg"
	"github.com/metacubex/tailscale/types/views"
)

// tailscaleExitNodeWatch is what a watch of the IPN bus that sets the exit node asks for.
const tailscaleExitNodeWatch = ipn.NotifyInitialState | ipn.NotifyPeerChanges

// exitNodeSetter sets the exit node of the config as the notifications of a watch of the IPN
// bus tell it to try: once the node runs, and after a try that failed, again on each change of
// a peer that offers exit routes. The node can run before the exit node is in its netmap or
// before its exit routes are approved, which a later map response brings, a delta one included;
// between such changes a try would fail the same way.
type exitNodeSetter struct {
	t   *Tailscale
	ctx context.Context
	// report, if set, gets the error of each try that failed, and "" once the exit node is set
	report  func(message string)
	running bool
	failed  string
}

// due reports whether n tells to try now.
func (s *exitNodeSetter) due(n *ipn.Notify) bool {
	if n.State != nil {
		s.running = *n.State == ipn.Running
		return s.running
	}
	return s.running && tailscaleOffersExitNode(n.PeersChanged)
}

// handle tries to set the exit node if n tells to. It reports whether the exit node is set or
// ctx ended, after which there is nothing left to do.
func (s *exitNodeSetter) handle(n *ipn.Notify) bool {
	if !s.due(n) {
		return false
	}
	err := s.t.applyExitNodePrefs(s.ctx)
	if s.ctx.Err() != nil {
		return true
	}
	if err == nil {
		if s.failed != "" {
			log.Infoln("[Tailscale](%s) set exit node %s", s.t.Name(), s.t.option.ExitNode)
		}
		if s.report != nil {
			s.report("")
		}
		return true
	}
	message := s.t.login.redact(fmt.Sprintf("set exit node: %v", err))
	if message != s.failed {
		s.failed = message
		log.Warnln("[Tailscale](%s) %s; trying again as peers change", s.t.Name(), message)
	}
	if s.report != nil {
		s.report(message)
	}
	return false
}

// tailscaleOffersExitNode reports whether any of the peers offers exit routes.
func tailscaleOffersExitNode(peers []*tailcfg.Node) bool {
	for _, peer := range peers {
		if tsaddr.ContainsExitRoutes(views.SliceOf(peer.AllowedIPs)) {
			return true
		}
	}
	return false
}
