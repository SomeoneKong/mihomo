package statistic

import (
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
)

// WithMatchInput keeps what the rules were matched against when the connection was
// made, before matching changed it, so it can be matched again under a newer config.
func WithMatchInput(input any) TrackerOption {
	return func(info *TrackerInfo) {
		info.matchInput = input
	}
}

// MatchInput returns what WithMatchInput kept, if anything.
func (info *TrackerInfo) MatchInput() any {
	return info.matchInput
}

// OutboundAdapter is implemented by the connections of outbounds.
type OutboundAdapter interface {
	OutboundAdapter() C.ProxyAdapter
}

// AdapterOf returns the adapter that made the connection of the tracker. The first one
// met is the proxy the connection was made by; going further down would reach the
// connection of a dialer proxy it was relayed through.
func AdapterOf(tracker Tracker) (C.ProxyAdapter, bool) {
	found, ok := N.FindUpstream[OutboundAdapter](tracker, nil)
	if !ok {
		return nil, false
	}
	adapter := found.OutboundAdapter()
	return adapter, adapter != nil
}
