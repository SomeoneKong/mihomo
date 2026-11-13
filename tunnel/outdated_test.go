package tunnel

import (
	"context"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/component/trie"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

// Only what judging uses is implemented; the embedded interfaces stay nil.

type testNode struct {
	C.Proxy
	name    string
	kind    C.AdapterType
	udp     bool
	builtin bool
}

func (n *testNode) Name() string                     { return n.name }
func (n *testNode) Type() C.AdapterType              { return n.kind }
func (n *testNode) Adapter() C.ProxyAdapter          { return n }
func (n *testNode) Unwrap(*C.Metadata, bool) C.Proxy { return nil }
func (n *testNode) SupportUDP() bool                 { return n.udp }
func (n *testNode) Builtin() bool                    { return n.builtin }

type testGroup struct {
	C.Proxy
	name        string
	members     []C.Proxy
	selected    C.Proxy
	udpDisabled bool
}

func (g *testGroup) Name() string            { return g.name }
func (g *testGroup) Type() C.AdapterType     { return C.Selector }
func (g *testGroup) Adapter() C.ProxyAdapter { return g }
func (g *testGroup) Proxies() []C.Proxy      { return g.members }
func (g *testGroup) UDPDisabled() bool       { return g.udpDisabled }
func (g *testGroup) Unwrap(*C.Metadata, bool) C.Proxy {
	return g.selected
}
func (g *testGroup) SupportUDP() bool {
	return !g.udpDisabled && g.selected.SupportUDP()
}

type testRule struct {
	C.Rule
	host    string
	adapter string
	match   func() // called on matching, to hold it up
}

func (r *testRule) Match(metadata *C.Metadata, _ C.RuleMatchHelper) (bool, string) {
	if r.match != nil {
		r.match()
	}
	return metadata.Host == r.host, r.adapter
}
func (r *testRule) Adapter() string { return r.adapter }

type testTracker struct {
	statistic.Tracker
	info *statistic.TrackerInfo
	made C.ProxyAdapter
}

func (t *testTracker) ID() string                      { return t.info.UUID.String() }
func (t *testTracker) Info() *statistic.TrackerInfo    { return t.info }
func (t *testTracker) OutboundAdapter() C.ProxyAdapter { return t.made }

func node(name string) *testNode {
	return &testNode{name: name, kind: C.Vless, udp: true}
}

func builtinDirect() *testNode {
	return &testNode{name: "DIRECT", kind: C.Direct, udp: true, builtin: true}
}

func group(name string, members ...C.Proxy) *testGroup {
	return &testGroup{name: name, members: members, selected: members[0]}
}

// useConfig publishes proxies and rules as a config would.
func useConfig(t *testing.T, rules []C.Rule, proxyList ...C.Proxy) {
	t.Helper()
	all := map[string]C.Proxy{
		"DIRECT": builtinDirect(),
		"REJECT": &testNode{name: "REJECT", kind: C.Reject, builtin: true},
	}
	for _, p := range proxyList {
		all[p.Name()] = p
	}
	UpdateProxies(all, map[string]P.ProxyProvider{})
	UpdateRules(rules, nil, map[string]P.RuleProvider{})
	t.Cleanup(func() {
		UpdateProxies(nil, nil)
		UpdateRules(nil, nil, nil)
	})
}

// made keeps what the rules saw of a connection, as its preprocessing does.
func made(metadata *C.Metadata) *matchInput {
	input := newMatchInput(metadata)
	_ = preHandleMetadataWith(metadata.Clone(), input.recording())
	return input
}

func trackerOf(metadata *C.Metadata, input *matchInput, by C.ProxyAdapter, chain ...string) *testTracker {
	info := &statistic.TrackerInfo{Metadata: metadata, Chain: chain}
	statistic.WithMatchInput(input)(info)
	return &testTracker{info: info, made: by}
}

// tracked makes a tracker for a connection made to host and carried along chain.
func tracked(network C.NetWork, host string, by C.ProxyAdapter, chain ...string) *testTracker {
	metadata := &C.Metadata{NetWork: network, Type: C.HTTP, Host: host, DstPort: 443}
	return trackerOf(metadata, made(metadata), by, chain...)
}

func TestJudgeKeepsWhatTheNewConfigCarriesTheSameWay(t *testing.T) {
	jp, hk := node("JP"), node("HK")
	rules := []C.Rule{&testRule{host: "a.com", adapter: "Proxy"}}
	useConfig(t, rules, jp, hk, group("Proxy", hk, jp)) // selects HK now
	if v := judge(context.Background(), tracked(C.TCP, "a.com", jp, "JP", "Proxy")); v != keep {
		t.Fatalf("a member the group no longer selects: %v", v)
	}
	// a node that doesn't carry UDP is selected now, the old path still does
	hk.udp = false
	if v := judge(context.Background(), tracked(C.UDP, "a.com", jp, "JP", "Proxy")); v != keep {
		t.Fatalf("UDP over a member the group no longer selects: %v", v)
	}
	// made by the DIRECT of the config before
	if v := judge(context.Background(), tracked(C.TCP, "b.com", builtinDirect(), "DIRECT")); v != keep {
		t.Fatalf("direct, as no rule matches it still: %v", v)
	}
}

func TestJudgeClosesWhatTheNewConfigCarriesOtherwise(t *testing.T) {
	jp, hk, us := node("JP"), node("HK"), node("US")
	connection := func() *testTracker { return tracked(C.TCP, "a.com", jp, "JP", "Proxy") }
	for name, use := range map[string]func(){
		"the group lost the member": func() {
			useConfig(t, []C.Rule{&testRule{host: "a.com", adapter: "Proxy"}}, hk, group("Proxy", hk))
		},
		"the rule goes to another group": func() {
			useConfig(t, []C.Rule{&testRule{host: "a.com", adapter: "US"}}, jp, us, group("Proxy", jp), group("US", us))
		},
		"the member was rebuilt": func() {
			useConfig(t, []C.Rule{&testRule{host: "a.com", adapter: "Proxy"}}, group("Proxy", node("JP")))
		},
		"the group was removed": func() {
			useConfig(t, []C.Rule{&testRule{host: "a.com", adapter: "JP"}}, jp)
		},
	} {
		use()
		if v := judge(context.Background(), connection()); v != outdated {
			t.Fatalf("%s: %v", name, v)
		}
	}
	// no rule matched it before, one does now
	useConfig(t, []C.Rule{&testRule{host: "b.com", adapter: "JP"}}, jp)
	if v := judge(context.Background(), tracked(C.TCP, "b.com", builtinDirect(), "DIRECT")); v != outdated {
		t.Fatalf("direct, though a rule matches it now: %v", v)
	}
	// UDP over a group that may no longer carry it
	pinned := group("Proxy", jp)
	pinned.udpDisabled = true
	useConfig(t, []C.Rule{&testRule{host: "a.com", adapter: "Proxy"}}, jp, pinned)
	if v := judge(context.Background(), tracked(C.UDP, "a.com", jp, "JP", "Proxy")); v != outdated {
		t.Fatalf("UDP over a group that disabled it: %v", v)
	}
}

func TestJudgeTellsMembersOfTheSameName(t *testing.T) {
	first, second := node("JP"), node("JP") // as two providers may have
	rules := []C.Rule{&testRule{host: "a.com", adapter: "Proxy"}}
	useConfig(t, rules, group("Proxy", first, second))
	if v := judge(context.Background(), tracked(C.TCP, "a.com", second, "JP", "Proxy")); v != keep {
		t.Fatalf("made by the second of two members of the same name: %v", v)
	}
	useConfig(t, rules, group("Proxy", first))
	if v := judge(context.Background(), tracked(C.TCP, "a.com", second, "JP", "Proxy")); v != outdated {
		t.Fatalf("the member that made it is gone: %v", v)
	}
}

func TestJudgeExemptsOnlyTheBuiltinOutbounds(t *testing.T) {
	// a provider may name a node of its own DIRECT
	named := &testNode{name: "DIRECT", kind: C.Vless, udp: true}
	rules := []C.Rule{&testRule{host: "a.com", adapter: "Proxy"}}
	useConfig(t, rules, group("Proxy", &testNode{name: "DIRECT", kind: C.Vless, udp: true}))
	if v := judge(context.Background(), tracked(C.TCP, "a.com", named, "DIRECT", "Proxy")); v != outdated {
		t.Fatalf("a rebuilt node named as a builtin outbound: %v", v)
	}
}

func TestJudgeUnderlyingByItsPathOnly(t *testing.T) {
	hk, sg := node("HK"), node("SG")
	underlying := func() *testTracker {
		metadata := &C.Metadata{NetWork: C.TCP, Type: C.INNER, Host: "relay.example", DstPort: 443}
		info := &statistic.TrackerInfo{Metadata: metadata, Chain: C.Chain{"HK", "Dialer"}}
		statistic.AsUnderlying(context.Background())(info)
		return &testTracker{info: info, made: hk}
	}
	// no rule takes it, which doesn't matter: it never went through the rules
	useConfig(t, nil, hk, sg, group("Dialer", sg, hk))
	if v := judge(context.Background(), underlying()); v != keep {
		t.Fatalf("a member still in the dialer group: %v", v)
	}
	useConfig(t, nil, sg, group("Dialer", sg))
	if v := judge(context.Background(), underlying()); v != outdated {
		t.Fatalf("a member the dialer group lost: %v", v)
	}
}

type testMapper struct {
	resolver.Enhancer
	hosts map[netip.Addr]string
	fake  bool
}

func (m *testMapper) MappingEnabled() bool     { return true }
func (m *testMapper) FakeIPEnabled() bool      { return m.fake }
func (m *testMapper) IsFakeIP(netip.Addr) bool { return m.fake }
func (m *testMapper) FindHostByIP(ip netip.Addr) (string, bool) {
	host, ok := m.hosts[ip]
	return host, ok
}

func useMapper(t *testing.T, mapper resolver.Enhancer) {
	previous := resolver.DefaultHostMapper
	resolver.DefaultHostMapper = mapper
	t.Cleanup(func() { resolver.DefaultHostMapper = previous })
}

func useHosts(t *testing.T, alias, of string) {
	tree := trie.New[resolver.HostValue]()
	value, _ := resolver.NewHostValueByDomain(of)
	_ = tree.Insert(alias, value)
	previous := resolver.DefaultHosts
	resolver.DefaultHosts = resolver.NewHosts(tree)
	t.Cleanup(func() { resolver.DefaultHosts = previous })
}

func TestJudgeMatchesTheDomainTheConnectionWasMadeTo(t *testing.T) {
	ip := netip.MustParseAddr("198.18.0.1")
	mapper := &testMapper{hosts: map[netip.Addr]string{ip: "a.com"}, fake: true}
	useMapper(t, mapper)
	jp := node("JP")
	metadata := &C.Metadata{NetWork: C.TCP, Type: C.TUN, DstIP: ip, DstPort: 443}
	connection := trackerOf(metadata, made(metadata), jp, "JP", "Proxy")
	useConfig(t, []C.Rule{&testRule{host: "a.com", adapter: "Proxy"}, &testRule{host: "b.com", adapter: "REJECT"}}, jp, group("Proxy", jp))

	// the fake IP went to another domain since
	mapper.hosts[ip] = "b.com"
	if v := judge(context.Background(), connection); v != keep {
		t.Fatalf("matched as the domain the IP stands for now: %v", v)
	}
	// the new config maps no IP
	useMapper(t, nil)
	if v := judge(context.Background(), connection); v != keep {
		t.Fatalf("matched without the domain the IP stood for: %v", v)
	}
}

func TestJudgeAppliesTheHostsOfTheNewConfig(t *testing.T) {
	ip := netip.MustParseAddr("192.0.2.1")
	useMapper(t, &testMapper{hosts: map[netip.Addr]string{ip: "a.com"}}) // as redir-host does
	jp := node("JP")
	metadata := &C.Metadata{NetWork: C.TCP, Type: C.TUN, DstIP: ip, DstPort: 443}
	connection := trackerOf(metadata, made(metadata), jp, "JP", "Proxy")
	useConfig(t, []C.Rule{&testRule{host: "a.com", adapter: "Proxy"}, &testRule{host: "b.com", adapter: "REJECT"}}, jp, group("Proxy", jp))
	if v := judge(context.Background(), connection); v != keep {
		t.Fatalf("no hosts apply: %v", v)
	}
	useHosts(t, "a.com", "b.com")
	if v := judge(context.Background(), connection); v != outdated {
		t.Fatalf("matched without the hosts of the new config: %v", v)
	}
}

func TestJudgeKeepsWhatSniffingFound(t *testing.T) {
	jp := node("JP")
	ip := netip.MustParseAddr("192.0.2.1")
	sniffed := func(override bool) *testTracker {
		metadata := &C.Metadata{NetWork: C.TCP, Type: C.HTTP, DstIP: ip, DstPort: 443}
		input := made(metadata)
		host, before := metadata.Host, metadata.DstIP
		metadata.SniffHost = "a.com"
		if override {
			metadata.Host, metadata.DstIP = "a.com", netip.Addr{}
		}
		input.sniffed(metadata, host, before)
		return trackerOf(metadata, input, jp, "JP", "Proxy")
	}
	useConfig(t, []C.Rule{&testRule{host: "a.com", adapter: "Proxy"}}, jp, group("Proxy", jp))
	if v := judge(context.Background(), sniffed(true)); v != keep {
		t.Fatalf("matched without the domain sniffing made the destination: %v", v)
	}
	// it kept the IP, which no rule takes to the proxy
	if v := judge(context.Background(), sniffed(false)); v != outdated {
		t.Fatalf("matched with a domain sniffing didn't make the destination: %v", v)
	}
}

type testRuleProvider struct {
	P.RuleProvider
	version uint64
}

func (p *testRuleProvider) StrategyVersion() uint64 { return p.version }

func TestProviderVersionsTellAnUpdate(t *testing.T) {
	useConfig(t, nil)
	rules := &testRuleProvider{version: 2}
	UpdateRules(nil, nil, map[string]P.RuleProvider{"rules": rules})
	before := currentProviderVersions()
	if !reflect.DeepEqual(before, currentProviderVersions()) || before.publishing() {
		t.Fatal("versions differ although no provider updated")
	}
	rules.version++
	if !currentProviderVersions().publishing() {
		t.Fatal("new rules being put in place went unnoticed")
	}
	rules.version++
	if reflect.DeepEqual(before, currentProviderVersions()) {
		t.Fatal("an update of a rule provider went unnoticed")
	}
}

func TestFindOutdatedConnectionsGivesUpOnSlowMatching(t *testing.T) {
	jp := node("JP")
	release := make(chan struct{})
	slow := &testRule{host: "a.com", adapter: "Proxy", match: func() { <-release }}
	connection := tracked(C.TCP, "a.com", jp, "JP", "Proxy")
	useConfig(t, []C.Rule{slow}, jp, group("Proxy", jp))
	// matching holds the config in use, which the cleanup of useConfig replaces
	t.Cleanup(func() { close(release) })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result := make(chan OutdatedResult, 1)
	go func() {
		_, found := FindOutdatedConnections(ctx, []statistic.Tracker{connection})
		result <- found
	}()
	select {
	case found := <-result:
		if found.Undetermined != 1 || found.Closed != 0 {
			t.Fatalf("a connection judged too late: %+v", found)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waited for matching past the deadline")
	}
}
