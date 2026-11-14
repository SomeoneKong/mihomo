package tunnel

import (
	"context"
	"slices"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

// proxyTable finds the proxies that chains name.
type proxyTable struct {
	// proxy finds a proxy or group of the configuration by name.
	proxy func(name string) C.Proxy
	// providerProxy finds a proxy of a proxy provider by name.
	providerProxy func(provider, name string) C.Proxy
}

var runningProxies = proxyTable{
	proxy: func(name string) C.Proxy {
		configMux.RLock()
		defer configMux.RUnlock()
		return proxies[name]
	},
	providerProxy: func(provider, name string) C.Proxy {
		configMux.RLock()
		pd := providers[provider]
		configMux.RUnlock()
		if pd == nil {
			return nil
		}
		for _, proxy := range pd.Proxies() {
			if proxy.Name() == name {
				return proxy
			}
		}
		return nil
	},
}

// recordDialers returns ctx with a recorder of the underlying connections dials under it make,
// and a function handing the connection those dials carry what they made, along with its
// dialer chains.
func recordDialers(ctx context.Context) (context.Context, func(C.Connection) statistic.TrackerOption) {
	ctx, recorder := statistic.WithDialerRecorder(ctx)
	return ctx, func(conn C.Connection) statistic.TrackerOption {
		layers := recorder.Layers()
		// dialerChains takes the layers it matches out of what it is given
		chains := statistic.WithDialerChains(dialerChains(conn.Chains(), conn.ProviderChains(), slices.Clone(layers), runningProxies))
		dialed := statistic.WithDialedLayers(layers)
		return func(info *statistic.TrackerInfo) {
			chains(info)
			dialed(info)
		}
	}
}

// dialerChains lists, layer by layer, the route the node that carried a connection reaches its
// server through, each layer being the dialer-proxy of the previous layer's node. A layer whose
// underlying connection was dialed for this connection is the route that connection took. A
// layer the node reused from an earlier connection is what its dialer-proxy selects now for the
// node's server, which differs only if a selection changed since.
func dialerChains(chain, providerChain C.Chain, dialed []statistic.DialedLayer, table proxyTable) []C.Chain {
	node := table.node(chain, providerChain)
	var layers []C.Chain
	visited := map[C.Proxy]struct{}{}
	for node != nil {
		target := node.ProxyInfo().DialerProxy
		if target == "" {
			break
		}
		if _, ok := visited[node]; ok {
			break // a dialer-proxy loop cannot dial
		}
		visited[node] = struct{}{}

		if index := slices.IndexFunc(dialed, func(layer statistic.DialedLayer) bool {
			return len(layer.Chain) > 0 && layer.Chain[len(layer.Chain)-1] == target
		}); index >= 0 {
			layers = append(layers, dialed[index].Chain)
			node = table.node(dialed[index].Chain, dialed[index].ProviderChain)
			dialed = slices.Delete(dialed, index, index+1)
			continue
		}

		dialer := table.proxy(target)
		if dialer == nil {
			break
		}
		metadata := &C.Metadata{}
		_ = metadata.SetRemoteAddress(node.Addr())
		var layer C.Chain
		layer, node = selection(dialer, metadata)
		layers = append(layers, layer)
	}
	return layers
}

// node finds the proxy a chain ends at, the one that dialed.
func (t proxyTable) node(chain, providerChain C.Chain) C.Proxy {
	if len(chain) == 0 {
		return nil
	}
	if len(providerChain) > 0 && providerChain[0] != "" {
		return t.providerProxy(providerChain[0], chain[0])
	}
	return t.proxy(chain[0])
}

// selection follows proxy through the groups it selects now for metadata, returning the path
// in the order chains use, the node first, and the node.
func selection(proxy C.Proxy, metadata *C.Metadata) (C.Chain, C.Proxy) {
	path := C.Chain{proxy.Name()}
	visited := map[C.Proxy]struct{}{proxy: {}}
	for {
		next := proxy.Unwrap(metadata, false)
		if next == nil {
			break
		}
		if _, ok := visited[next]; ok {
			break // a group loop cannot dial
		}
		visited[next] = struct{}{}
		proxy = next
		path = append(path, proxy.Name())
	}
	slices.Reverse(path)
	return path, proxy
}
