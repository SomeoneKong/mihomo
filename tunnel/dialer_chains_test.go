package tunnel

import (
	"fmt"
	"testing"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"

	"github.com/stretchr/testify/assert"
)

// fakeProxy implements only what dialerChains uses; other C.Proxy methods are not called.
type fakeProxy struct {
	C.Proxy
	name   string
	addr   string
	dialer string
	// pick selects a member for a group; nil for a node.
	pick func(*C.Metadata) C.Proxy
}

func (p *fakeProxy) Name() string { return p.name }

func (p *fakeProxy) Addr() string { return p.addr }

func (p *fakeProxy) ProxyInfo() C.ProxyInfo { return C.ProxyInfo{DialerProxy: p.dialer} }

func (p *fakeProxy) Unwrap(metadata *C.Metadata, _ bool) C.Proxy {
	if p.pick == nil {
		return nil
	}
	return p.pick(metadata)
}

func selects(member C.Proxy) func(*C.Metadata) C.Proxy {
	return func(*C.Metadata) C.Proxy { return member }
}

func tableOf(proxies []*fakeProxy, provided map[string][]*fakeProxy) proxyTable {
	find := func(list []*fakeProxy, name string) C.Proxy {
		for _, proxy := range list {
			if proxy.name == name {
				return proxy
			}
		}
		return nil
	}
	return proxyTable{
		proxy:         func(name string) C.Proxy { return find(proxies, name) },
		providerProxy: func(provider, name string) C.Proxy { return find(provided[provider], name) },
	}
}

func TestDialerChains(t *testing.T) {
	jump := &fakeProxy{name: "jump", addr: "jump.example:443"}
	relayA := &fakeProxy{name: "relay-a", addr: "a.example:443", dialer: "jump"}
	relayB := &fakeProxy{name: "relay-b", addr: "b.example:443"}
	// round-robin has moved on to relay-b since relay-a carried the dial
	relay := &fakeProxy{name: "relay", pick: selects(relayB)}
	// picks by the server it is asked to reach, like consistent-hashing
	hash := &fakeProxy{name: "hash", pick: func(metadata *C.Metadata) C.Proxy {
		if metadata.Host == "hk.example" {
			return relayA
		}
		return relayB
	}}
	hk := &fakeProxy{name: "hk", addr: "hk.example:443", dialer: "relay"}
	hashed := &fakeProxy{name: "hashed", addr: "hk.example:443", dialer: "hash"}
	plain := &fakeProxy{name: "plain", addr: "plain.example:443"}
	provided := &fakeProxy{name: "provided", addr: "p.example:443", dialer: "jump"}
	table := tableOf(
		[]*fakeProxy{jump, relayA, relayB, relay, hash, hk, hashed, plain},
		map[string][]*fakeProxy{"subscription": {provided}},
	)

	t.Run("no dialer", func(t *testing.T) {
		assert.Nil(t, dialerChains(C.Chain{"plain", "group"}, C.Chain{"", ""}, nil, table))
	})

	t.Run("a dialed layer is the route it took", func(t *testing.T) {
		dialed := []statistic.DialedLayer{{Chain: C.Chain{"relay-a", "relay"}, ProviderChain: C.Chain{"", ""}}}
		assert.Equal(t,
			[]C.Chain{{"relay-a", "relay"}, {"jump"}},
			dialerChains(C.Chain{"hk", "group"}, C.Chain{"", ""}, dialed, table))
	})

	t.Run("a reused layer is what the dialer selects now", func(t *testing.T) {
		assert.Equal(t,
			[]C.Chain{{"relay-b", "relay"}},
			dialerChains(C.Chain{"hk", "group"}, C.Chain{"", ""}, nil, table))
	})

	t.Run("a reused layer is selected for the node's server", func(t *testing.T) {
		assert.Equal(t,
			[]C.Chain{{"relay-a", "hash"}, {"jump"}},
			dialerChains(C.Chain{"hashed"}, C.Chain{""}, nil, table))
	})

	t.Run("node from a provider", func(t *testing.T) {
		assert.Equal(t,
			[]C.Chain{{"jump"}},
			dialerChains(C.Chain{"provided", "group"}, C.Chain{"subscription", ""}, nil, table))
	})

	t.Run("unknown node", func(t *testing.T) {
		assert.Nil(t, dialerChains(C.Chain{"gone"}, C.Chain{""}, nil, table))
	})

	t.Run("dialer loop ends", func(t *testing.T) {
		a := &fakeProxy{name: "a", dialer: "b"}
		b := &fakeProxy{name: "b", dialer: "a"}
		assert.Equal(t,
			[]C.Chain{{"b"}, {"a"}},
			dialerChains(C.Chain{"a"}, nil, nil, tableOf([]*fakeProxy{a, b}, nil)))
	})

	t.Run("group loop ends", func(t *testing.T) {
		x := &fakeProxy{name: "x"}
		y := &fakeProxy{name: "y", pick: selects(x)}
		x.pick = selects(y)
		node := &fakeProxy{name: "n", dialer: "x"}
		assert.Equal(t,
			[]C.Chain{{"y", "x"}},
			dialerChains(C.Chain{"n"}, nil, nil, tableOf([]*fakeProxy{node, x, y}, nil)))
	})

	t.Run("deep groups", func(t *testing.T) {
		member := C.Proxy(jump)
		proxies := []*fakeProxy{jump}
		want := C.Chain{"jump"}
		for i := 0; i < 20; i++ {
			group := &fakeProxy{name: fmt.Sprintf("g%d", i), pick: selects(member)}
			proxies = append(proxies, group)
			want = append(want, group.name)
			member = group
		}
		node := &fakeProxy{name: "n", dialer: "g19"}
		proxies = append(proxies, node)
		assert.Equal(t, []C.Chain{want}, dialerChains(C.Chain{"n"}, nil, nil, tableOf(proxies, nil)))
	})
}
