package tunnel

import (
	"context"
	"net/netip"
	"reflect"
	"sync"

	"github.com/metacubex/mihomo/component/resolver"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

// ipMapping answers what preprocessing asks about the IP of a connection.
type ipMapping interface {
	// needLookup reports whether the domain the IP stands for is to be looked up.
	needLookup(metadata *C.Metadata) bool
	findHost(ip netip.Addr) (string, bool)
	isFakeIP(ip netip.Addr) bool
}

// liveMapping answers by the DNS of the config in use.
type liveMapping struct{}

func (liveMapping) needLookup(metadata *C.Metadata) bool  { return needLookupIP(metadata) }
func (liveMapping) findHost(ip netip.Addr) (string, bool) { return resolver.FindHostByIP(ip) }
func (liveMapping) isFakeIP(ip netip.Addr) bool           { return resolver.IsFakeIP(ip) }

// matchInput is what the rules saw of a connection, kept to match it again under a newer
// config. What the IP stood for when the connection was made and what sniffing found
// belong to the connection: a mapping may have gone to another domain since, and a newer
// config may map in another way. The hosts are applied again, as a newer config may have
// other ones.
type matchInput struct {
	metadata *C.Metadata // before preprocessing

	looked bool // the answers preprocessing got
	host   string
	found  bool
	fake   bool

	sniffHost     string // the domain sniffing a TCP connection found after preprocessing
	sniffOverride bool   // which it made the destination
}

func newMatchInput(metadata *C.Metadata) *matchInput {
	return &matchInput{metadata: metadata.Clone()}
}

// recording answers by the DNS of the config in use and keeps the answers.
func (in *matchInput) recording() ipMapping { return (*recordingMapping)(in) }

type recordingMapping matchInput

func (r *recordingMapping) needLookup(metadata *C.Metadata) bool {
	r.looked = needLookupIP(metadata)
	return r.looked
}

func (r *recordingMapping) findHost(ip netip.Addr) (string, bool) {
	r.host, r.found = resolver.FindHostByIP(ip)
	return r.host, r.found
}

func (r *recordingMapping) isFakeIP(ip netip.Addr) bool {
	r.fake = resolver.IsFakeIP(ip)
	return r.fake
}

// replaying answers what preprocessing got when the connection was made.
type replayingMapping matchInput

func (r *replayingMapping) needLookup(*C.Metadata) bool        { return r.looked }
func (r *replayingMapping) findHost(netip.Addr) (string, bool) { return r.host, r.found }
func (r *replayingMapping) isFakeIP(netip.Addr) bool           { return r.fake }

// sniffed notes what sniffing wrote back to the metadata, which had host and ip before.
// Making the domain found the destination sets the host and clears the IP; otherwise both
// stay. Where neither changes, as when the host was the domain found already and there
// was no IP, the destination is taken as kept: the rules saw the same either way.
func (in *matchInput) sniffed(metadata *C.Metadata, host string, ip netip.Addr) {
	in.sniffHost = metadata.SniffHost
	in.sniffOverride = metadata.Host != host || metadata.DstIP != ip
}

// replay prepares the metadata as the rules would see the connection now. The process is
// the one of the connection, which is not looked up again: it may have exited since, and
// its port gone to another process.
func (in *matchInput) replay(made *C.Metadata) (*C.Metadata, error) {
	metadata := in.metadata.Clone()
	err := preHandleMetadataWith(metadata, (*replayingMapping)(in))
	if in.sniffHost != "" { // as sniffing writes it back
		metadata.SniffHost = in.sniffHost
		if in.sniffOverride {
			metadata.Host = in.sniffHost
			metadata.DstIP = netip.Addr{}
		}
		metadata.DNSMode = C.DNSNormal
		err = nil
	}
	metadata.Process, metadata.ProcessPath, metadata.Uid = made.Process, made.ProcessPath, made.Uid
	return metadata, err
}

// pinnedPath is the path an existing connection took from the policy a rule chose to the
// proxy that made it, in the proxies of the config in use. Matching the connection again
// follows it where the groups would decide by what they select now, so a group that
// selects another of its members doesn't count as a change. Matching along a path doesn't
// look up the process, which the connection had.
type pinnedPath struct {
	policy string
	hops   []C.Proxy // from the policy to the proxy that made the connection
}

// unwrap returns the next hop from a, reached from the policy a rule chose.
func (p *pinnedPath) unwrap(policy string, a C.Proxy, metadata *C.Metadata) C.Proxy {
	if p == nil || policy != p.policy {
		return a.Unwrap(metadata, false)
	}
	for i, hop := range p.hops[:len(p.hops)-1] {
		if hop == a {
			return p.hops[i+1]
		}
	}
	return nil
}

// supportUDP reports whether a, reached from the policy a rule chose, carries UDP.
func (p *pinnedPath) supportUDP(policy string, a C.Proxy) bool {
	if p == nil || policy != p.policy {
		return a.SupportUDP()
	}
	for _, group := range p.hops[:len(p.hops)-1] {
		if g, ok := group.Adapter().(interface{ UDPDisabled() bool }); ok && g.UDPDisabled() {
			return false
		}
	}
	return p.hops[len(p.hops)-1].SupportUDP()
}

// pathOf finds the hops of the chain of tracker, from its policy to the proxy that made the
// connection, among the proxies in use: each group must still be a group that has the next
// hop as a member, whichever it selects now, and the last hop must be the proxy that made
// the connection. Members of a group may share a name, as those of two providers can.
func pathOf(tracker statistic.Tracker, chain C.Chain) ([]C.Proxy, bool) {
	configMux.RLock()
	defer configMux.RUnlock()
	hop, ok := proxies[chain[len(chain)-1]]
	if !ok {
		return nil, false
	}
	hops := []C.Proxy{hop}
	for i := len(chain) - 2; i >= 0; i-- {
		if hop.Type() == C.Relay {
			return nil, false
		}
		group, ok := hop.Adapter().(interface{ Proxies() []C.Proxy })
		if !ok {
			return nil, false
		}
		hop = nil
		for _, member := range group.Proxies() {
			if member.Name() == chain[i] && (i > 0 || madeBy(tracker, member)) {
				hop = member
				break
			}
		}
		if hop == nil {
			return nil, false
		}
		hops = append(hops, hop)
	}
	if len(hops) == 1 && !madeBy(tracker, hop) {
		return nil, false
	}
	return hops, true
}

func builtin(adapter C.ProxyAdapter) bool {
	b, ok := adapter.(interface{ Builtin() bool })
	return ok && b.Builtin()
}

// madeBy reports whether the connection of tracker was made by proxy. The outbounds a
// config makes for the names it reserves, as DIRECT, are made anew by each config, and do
// the same in each.
func madeBy(tracker statistic.Tracker, proxy C.Proxy) bool {
	made, ok := statistic.AdapterOf(tracker)
	if !ok {
		return builtin(proxy.Adapter())
	}
	if builtin(made) {
		return builtin(proxy.Adapter()) && made.Name() == proxy.Name() && made.Type() == proxy.Type()
	}
	for a := proxy.Adapter(); a != nil; {
		if a == made {
			return true
		}
		wrapper, ok := a.(interface{ WrappedAdapter() C.ProxyAdapter })
		if !ok {
			return false
		}
		a = wrapper.WrappedAdapter()
	}
	return false
}

type verdict int

const (
	undetermined verdict = iota
	keep
	outdated
)

// judge decides whether the config in use carries the connection the way it is carried.
func judge(ctx context.Context, tracker statistic.Tracker) verdict {
	info := tracker.Info()
	chain := info.Chain
	if len(chain) == 0 {
		return undetermined
	}
	hops, ok := pathOf(tracker, chain)
	if !ok {
		return outdated
	}
	input, _ := info.MatchInput().(*matchInput)
	// a connection a dialer proxy made to carry another one, or one mihomo made for
	// itself, didn't go through the rules
	if info.Underlying() || info.Metadata.Type == C.INNER || input == nil {
		return keep
	}
	metadata, err := input.replay(info.Metadata)
	if err != nil {
		return undetermined
	}
	path := &pinnedPath{policy: chain[len(chain)-1], hops: hops}
	proxy, _, err := resolveMetadataWith(ctx, metadata, path)
	if ctx.Err() != nil {
		return undetermined
	}
	if err != nil || proxy.Name() != path.policy {
		return outdated
	}
	return keep
}

// OutdatedResult counts what FindOutdatedConnections found.
type OutdatedResult struct {
	Closed       int `json:"closed"`
	Kept         int `json:"kept"`
	Undetermined int `json:"undetermined"`
}

// outdatedWorkers bounds the connections judged at once; matching may wait on DNS.
const outdatedWorkers = 16

// FindOutdatedConnections finds which of trackers the config in use would not carry the
// way they are carried, as after switching to another config: the rules choose another
// policy for them, a group on their path no longer has the next hop as a member, or the
// proxy that made them was rebuilt. The caller holds off config updates while it runs.
//
// Connections not judged when ctx is done are kept. Judging that has not ended by then is
// left to end on its own and its verdict dropped: matching can take as long as a rule
// does, and the caller must not hold off config updates for that long. All connections
// are kept if a provider updated in between, as they were judged against more than one
// state.
func FindOutdatedConnections(ctx context.Context, trackers []statistic.Tracker) ([]statistic.Tracker, OutdatedResult) {
	before := currentProviderVersions()

	var (
		mu       sync.Mutex
		verdicts = make([]verdict, len(trackers))
		over     bool
		wg       sync.WaitGroup
	)
	next := make(chan int)
	for w := 0; w < outdatedWorkers && w < len(trackers); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				v := judge(ctx, trackers[i])
				mu.Lock()
				if !over {
					verdicts[i] = v
				}
				mu.Unlock()
			}
		}()
	}
	go func() {
		defer close(next)
		for i := range trackers {
			select {
			case next <- i:
			case <-ctx.Done():
				return
			}
		}
	}()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
	mu.Lock()
	over = true
	mu.Unlock()

	var result OutdatedResult
	if before.publishing() || !reflect.DeepEqual(currentProviderVersions(), before) {
		result.Undetermined = len(trackers)
		return nil, result
	}
	var found []statistic.Tracker
	for i, v := range verdicts {
		switch v {
		case outdated:
			found = append(found, trackers[i])
			result.Closed++
		case keep:
			result.Kept++
		default:
			result.Undetermined++
		}
	}
	return found, result
}

// providerVersions tells whether a provider updated: one of proxies changes the members
// of groups, one of rules changes what the rules match.
type providerVersions struct {
	proxies map[string]uint32
	rules   map[string]uint64
}

// publishing reports whether a rule provider was putting new rules in place.
func (v providerVersions) publishing() bool {
	for _, version := range v.rules {
		if version%2 != 0 {
			return true
		}
	}
	return false
}

func currentProviderVersions() providerVersions {
	configMux.RLock()
	defer configMux.RUnlock()
	versions := providerVersions{proxies: map[string]uint32{}, rules: map[string]uint64{}}
	for name, pd := range providers {
		versions.proxies[name] = pd.Version()
	}
	for name, pd := range ruleProviders {
		if published, ok := pd.(interface{ StrategyVersion() uint64 }); ok {
			versions.rules[name] = published.StrategyVersion()
		}
	}
	return versions
}
