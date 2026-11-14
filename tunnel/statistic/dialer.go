package statistic

import (
	"context"
	"slices"
	"sync"

	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
)

// DialedLayer is the route an underlying connection took, as its chains name it.
type DialedLayer struct {
	Chain         C.Chain
	ProviderChain C.Chain

	adapter C.ProxyAdapter // the outbound that made the connection, if known
}

// DialerRecorder collects the underlying connections proxy dialers make under a context.
type DialerRecorder struct {
	mutex  sync.Mutex
	layers []DialedLayer
}

type dialerRecorderKey struct{}

// WithDialerRecorder returns a context under which proxy dialers report the underlying
// connections they make to the returned recorder.
func WithDialerRecorder(ctx context.Context) (context.Context, *DialerRecorder) {
	recorder := &DialerRecorder{}
	return context.WithValue(ctx, dialerRecorderKey{}, recorder), recorder
}

// Layers returns the underlying connections recorded so far, in the order they were made:
// one a dial made through another is made before it, so the deepest comes first.
func (r *DialerRecorder) Layers() []DialedLayer {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return slices.Clone(r.layers)
}

func (r *DialerRecorder) record(info *TrackerInfo, adapter C.ProxyAdapter) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.layers = append(r.layers, DialedLayer{Chain: info.Chain, ProviderChain: info.ProviderChain, adapter: adapter})
}

// WithDialedLayers hands a connection the underlying connections its dial made, which the
// route its usage is counted under is rebuilt from.
func WithDialedLayers(layers []DialedLayer) TrackerOption {
	return func(info *TrackerInfo) {
		info.dialedLayers = layers
	}
}

func adapterOfConn(conn any) C.ProxyAdapter {
	if found, ok := N.FindUpstream[OutboundAdapter](conn, nil); ok {
		return found.OutboundAdapter()
	}
	return nil
}

// usageDialers rebuilds, layer by layer, the dialer proxies a connection made by adapter
// reached its server through, from what its dial made rather than from the config in use:
// each layer is the underlying connection made through the dialer proxy the previous
// layer's adapter names, its node first. An underlying connection is made before the one
// made through it, so each layer is looked for only among the connections made before the
// previous layer's, the latest first; the same group may thus appear in several layers,
// and every connection is used at most once. A layer the dial made no connection for, as
// when the node reused a session or its transport dialed under a context of its own, ends
// the route with the dialer proxy's name and an unknown node.
func usageDialers(adapter C.ProxyAdapter, dialed []DialedLayer) [][]string {
	var route [][]string
	for adapter != nil {
		target := adapter.ProxyInfo().DialerProxy
		if target == "" {
			break
		}
		index := len(dialed) - 1
		for index >= 0 && !endsWith(dialed[index].Chain, target) {
			index--
		}
		if index < 0 {
			route = append(route, []string{"", target})
			break
		}
		route = append(route, slices.Clone(dialed[index].Chain))
		adapter = dialed[index].adapter
		dialed = dialed[:index]
	}
	return route
}

func endsWith(chain C.Chain, name string) bool {
	return len(chain) > 0 && chain[len(chain)-1] == name
}
