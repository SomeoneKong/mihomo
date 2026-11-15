package outboundgroup

import (
	"sort"
	"sync"
	"time"

	"github.com/metacubex/mihomo/adapter/provider"
	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"

	"golang.org/x/exp/slices"
)

func init() {
	provider.SetProxiesChanged(ProvidersChanged)
}

// lifecycle tracks the on-demand groups of the config in use.
var lifecycle = struct {
	sync.Mutex
	proxies    map[string]C.Proxy
	groups     map[string]*onDemand
	publishing bool
	census     time.Duration
	// topology is the members of every group, as last published in a config context.
	topology []C.Proxy
}{groups: map[string]*onDemand{}}

func onDemandOf(p C.Proxy) *onDemand {
	if f, ok := p.Adapter().(*Fallback); ok {
		return f.od
	}
	return nil
}

func onDemandGroups(proxies map[string]C.Proxy) map[string]*onDemand {
	groups := map[string]*onDemand{}
	for name, p := range proxies {
		if od := onDemandOf(p); od != nil {
			groups[name] = od
		}
	}
	return groups
}

// BeginPublish is called before a new config publishes its proxies. The groups of the config
// in use stop asking for tests; the groups of the new config start receiving the results of
// tests, and route by what is known, but ask for no test until Activate.
func BeginPublish(proxies map[string]C.Proxy, providers map[string]P.ProxyProvider) {
	lifecycle.Lock()
	lifecycle.publishing = true
	lifecycle.Unlock()
	probe.Default.BeginPublish()
	probe.Default.SetLookup(func(name string) C.Proxy { return proxies[name] })
	probe.Default.SetNodeLookup(nodeLookup(proxies, providers))
	lifecycle.Lock()
	old := lifecycle.groups
	lifecycle.Unlock()
	// The groups listed by others come first, so that the groups listing them select by
	// what they published.
	for _, name := range publishOrder(proxies) {
		od := onDemandOf(proxies[name])
		// The member the group of the same name dialed last, for the switch of the first
		// dial of this one.
		if prev := old[name]; prev != nil && prev != od {
			if dialed := prev.dialed.Load(); dialed != nil {
				od.dialed.CompareAndSwap(nil, dialed)
			}
		}
		od.publish()
	}
}

// nodeLookup finds by name the node a route went through, among the proxies of a config and
// those of its providers as they are when asked: a name more than one node goes by tells
// nothing. Groups are no nodes: a group may go by the name of the node it holds.
func nodeLookup(proxies map[string]C.Proxy, providers map[string]P.ProxyProvider) func(string) C.Proxy {
	isGroup := func(p C.Proxy) bool {
		_, ok := p.Adapter().(ProxyGroup)
		return ok
	}
	return func(name string) C.Proxy {
		found := proxies[name]
		if found != nil && isGroup(found) {
			found = nil
		}
		for _, pd := range providers {
			for _, p := range pd.Proxies() {
				if p.Name() != name || p == found || isGroup(p) {
					continue
				}
				if found != nil {
					return nil
				}
				found = p
			}
		}
		return found
	}
}

// publishOrder lists the on-demand groups of proxies, each after the on-demand groups among
// its members, otherwise by name.
func publishOrder(proxies map[string]C.Proxy) []string {
	names := make([]string, 0, len(proxies))
	for name, p := range proxies {
		if onDemandOf(p) != nil {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	var order []string
	seen := map[*onDemand]bool{}
	var visit func(od *onDemand)
	visit = func(od *onDemand) {
		if seen[od] {
			return
		}
		seen[od] = true
		for _, member := range od.f.GetProxies(false) {
			if child := onDemandOf(member); child != nil {
				visit(child)
			}
		}
		if p := proxies[od.name()]; p != nil && onDemandOf(p) == od {
			order = append(order, od.name())
		}
	}
	for _, name := range names {
		visit(onDemandOf(proxies[name]))
	}
	return order
}

// Activate is called once the new config loaded its providers and restored the pinned
// members: the new groups take over the state of the groups of the same name they replace,
// and start asking for tests. censusHours is the census interval, 0 for none.
func Activate(proxies map[string]C.Proxy, censusHours int) {
	lifecycle.Lock()
	defer lifecycle.Unlock()
	lifecycle.publishing = false
	old := lifecycle.groups
	groups := onDemandGroups(proxies)
	lifecycle.proxies = proxies
	lifecycle.groups = groups
	lifecycle.census = time.Duration(censusHours) * time.Hour

	// The members of the new groups as the loaded providers have them, which their
	// bindings are moved by.
	version := topologyVersion.Add(1)
	for _, od := range groups {
		od.updateMemberSet(od.f.GetProxies(false), version)
	}
	// The pinned members restored and the providers loaded change what the groups listed by
	// others publish: they take it in first, the groups listing them select by it then.
	order := make([]*onDemand, 0, len(groups))
	for _, name := range publishOrder(proxies) {
		order = append(order, groups[name])
	}
	settleGroups(order, settleWait)
	keys, retain := referencedKeys(proxies, groups)
	lifecycle.topology = topologyOf(proxies)
	probe.Default.Records().SetConfig(configContext(proxies, groups, "load", lifecycle.census))
	// The census is set to the new members before the old ones are dropped, so that it does
	// not ask for them again.
	probe.Default.SetCensus(lifecycle.census, censusKeys(groups))
	// The old groups hear of no test from here on; what they heard before is handled before
	// their state is handed over. The new groups ask for tests only once they took it over.
	probe.Default.Activate(keys, retain)
	for name, prev := range old {
		od := groups[name]
		if od == prev {
			continue
		}
		state := prev.retire(od)
		migrateBindings(prev, od)
		if od != nil {
			od.post(inheritNote{state: state})
		}
	}
	for _, od := range groups {
		od.post(activateNote{})
	}
}

// settleWait is how long the activation of a config waits for each group to take in what it
// was told.
const settleWait = 500 * time.Millisecond

// censusKeys lists what the census tests: the members of the on-demand groups but the
// built-in outbounds they do not test.
func censusKeys(groups map[string]*onDemand) []probe.Key {
	var keys []probe.Key
	for _, od := range groups {
		members := od.f.GetProxies(false)
		probed := probe.ProbedDirects(members)
		for _, member := range members {
			if !probe.IsBuiltin(member) || probed[member] {
				keys = append(keys, probe.Key{Target: member, Cond: od.cond})
			}
		}
	}
	return keys
}

// topologyOf lists the members of every group, the groups in name order, each group followed
// by its members in order.
func topologyOf(proxies map[string]C.Proxy) []C.Proxy {
	names := make([]string, 0, len(proxies))
	for name, p := range proxies {
		if _, ok := p.Adapter().(ProxyGroup); ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	var topology []C.Proxy
	for _, name := range names {
		p := proxies[name]
		topology = append(topology, p)
		topology = append(topology, p.Adapter().(ProxyGroup).Proxies()...)
	}
	return topology
}

// referencedKeys lists the evidence the on-demand groups read, and every proxy the config
// reaches, whose object numbers are kept.
func referencedKeys(proxies map[string]C.Proxy, groups map[string]*onDemand) ([]probe.Key, []C.Proxy) {
	var keys []probe.Key
	seen := map[C.Proxy]struct{}{}
	var retain []C.Proxy
	add := func(p C.Proxy) {
		if _, ok := seen[p]; !ok {
			seen[p] = struct{}{}
			retain = append(retain, p)
		}
	}
	for _, p := range proxies {
		add(p)
		if group, ok := p.Adapter().(ProxyGroup); ok {
			for _, member := range group.Proxies() {
				add(member)
			}
		}
	}
	for _, od := range groups {
		for _, member := range od.f.GetProxies(false) {
			keys = append(keys, probe.Key{Target: member, Cond: od.cond})
		}
	}
	return keys, retain
}

// ProvidersChanged is called after a provider replaced its proxies while running.
func ProvidersChanged() {
	lifecycle.Lock()
	defer lifecycle.Unlock()
	if lifecycle.publishing || lifecycle.proxies == nil {
		return
	}
	topology := topologyOf(lifecycle.proxies)
	if slices.Equal(topology, lifecycle.topology) {
		return
	}
	lifecycle.topology = topology
	// The groups refer to their new members before the evidence of the old ones is dropped,
	// so that what a group asked for its old members before cannot start again.
	version := topologyVersion.Add(1)
	for _, od := range lifecycle.groups {
		members := od.f.GetProxies(false)
		od.updateMemberSet(members, version)
		od.post(topologyNote{members: members})
	}
	keys, retain := referencedKeys(lifecycle.proxies, lifecycle.groups)
	probe.Default.Records().SetConfig(configContext(lifecycle.proxies, lifecycle.groups, "provider", lifecycle.census))
	probe.Default.SetCensus(lifecycle.census, censusKeys(lifecycle.groups))
	probe.Default.Prune(keys, retain)
}

func configContext(proxies map[string]C.Proxy, groups map[string]*onDemand, source string, census time.Duration) map[string]any {
	coord := probe.Default
	names := make([]string, 0, len(proxies))
	for name := range proxies {
		names = append(names, name)
	}
	sort.Strings(names)
	nodes := map[uint64]map[string]any{}
	kind := func(p C.Proxy) string {
		if probe.IsBuiltin(p) {
			return "builtin"
		}
		if _, ok := p.Adapter().(ProxyGroup); ok {
			return "group"
		}
		id := coord.ObjectID(p)
		if _, ok := nodes[id]; !ok {
			info := p.ProxyInfo()
			node := map[string]any{
				"object": id, "provider": info.ProviderName, "name": p.Name(), "type": p.Type().String(),
				"addr": p.Addr(), "dialerProxy": info.DialerProxy, "pool": "",
			}
			if owner, ok := p.(C.TrafficPoolOwner); ok {
				node["pool"] = owner.TrafficPool()
			}
			nodes[id] = node
		}
		return "node"
	}
	var onDemandList, otherList []any
	for _, name := range names {
		p := proxies[name]
		group, isGroup := p.Adapter().(ProxyGroup)
		if !isGroup {
			kind(p)
			continue
		}
		var members []any
		list := group.Proxies()
		var probed map[C.Proxy]bool
		if groups[name] != nil {
			probed = probe.ProbedDirects(list)
		}
		for _, member := range list {
			entry := map[string]any{"name": member.Name(), "object": coord.ObjectID(member), "kind": kind(member)}
			if probed[member] {
				entry["probed"] = true
			}
			members = append(members, entry)
		}
		if od := groups[name]; od != nil {
			od.mutex.Lock()
			pinned := od.pinned
			od.mutex.Unlock()
			if pinned == "" {
				pinned = od.f.selected
			}
			onDemandList = append(onDemandList, map[string]any{
				"name": name, "url": od.cond.URL, "expectedStatus": od.cond.Expected,
				"timeout": od.cond.Timeout.Milliseconds(), "members": members, "pinned": pinned,
				"strictOrder": od.strict, "delegable": od.isDelegable(), "order": od.ordering,
			})
			continue
		}
		otherList = append(otherList, map[string]any{"name": name, "type": p.Type().String(), "members": members})
	}
	ids := make([]uint64, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	nodeList := make([]any, 0, len(ids))
	for _, id := range ids {
		nodeList = append(nodeList, nodes[id])
	}
	return map[string]any{
		"source":      source,
		"groups":      onDemandList,
		"otherGroups": otherList,
		"nodes":       nodeList,
		"censusHours": int(census / time.Hour),
	}
}

// inheritedState is what a group hands to the group of the same name replacing it.
type inheritedState struct {
	cond        probe.Condition
	lastDial    int64
	lastData    int64
	lastActive  int64
	lastSwitch  time.Time
	repairUntil time.Time
	dialed      *dialedMember
	carried     *carriedMember
	search      *searchState
	members     map[C.Proxy]memberState
	// the standings of the members and what they were found by
	quality      map[C.Proxy]*memberQuality
	degradations []*degradation
	// the connections of the minutes not summed up yet, which the new group sums up with its own
	summaries  map[summaryKey]*summary
	summarized int64
	dialCounts map[dialKey]*dialCounts
	// what the rounds comparing members found, and the preference taken on it
	prefer *preferState
}

type inheritNote struct{ state inheritedState }

func (od *onDemand) export() inheritedState {
	od.mutex.Lock()
	defer od.mutex.Unlock()
	state := inheritedState{
		cond:        od.cond,
		lastDial:    od.lastDial.Load(),
		lastData:    od.lastData.Load(),
		lastActive:  od.lastActive.Load(),
		lastSwitch:  od.lastSwitch,
		repairUntil: od.repairUntil,
		dialed:      od.dialed.Load(),
		carried:     od.carried.Load(),
		members:     make(map[C.Proxy]memberState, len(od.states)),
	}
	if od.search != nil {
		search := *od.search
		state.search = &search
	}
	for p, ms := range od.states {
		state.members[p] = *ms
	}
	state.quality = make(map[C.Proxy]*memberQuality, len(od.quality))
	for p, q := range od.quality {
		state.quality[p] = q.clone()
	}
	for _, d := range od.degradations {
		copied := *d
		state.degradations = append(state.degradations, &copied)
	}
	state.summaries, state.summarized = od.summaries, od.summarized
	od.summaries = map[summaryKey]*summary{}
	state.dialCounts = od.dialCounts
	od.dialCounts = map[dialKey]*dialCounts{}
	state.prefer = od.prefer.clone()
	return state
}

// inheritSummaries adds the connections the group replaced counted to those of this one.
func (od *onDemand) inheritSummaries(state inheritedState) {
	if state.summarized > od.summarized {
		od.summarized = state.summarized
	}
	for key, s := range state.summaries {
		if !od.isMember(key.member) {
			continue
		}
		own := od.summaries[key]
		if own == nil {
			od.summaries[key] = s
			continue
		}
		own.dialed += s.dialed
		own.delivered += s.delivered
		own.firstData += s.firstData
		own.dialFailed += s.dialFailed
		own.canceled += s.canceled
		own.undecided += s.undecided
		for phase, count := range s.failed {
			own.failed[phase] += count
		}
	}
}

// hasDegradation reports whether this group attributes the test of a degradation already, or
// did: both groups heard of it.
func (od *onDemand) hasDegradation(d *degradation) bool {
	for _, own := range od.degradations {
		if own.member == d.member && own.exec == d.exec {
			return true
		}
	}
	key := probe.Key{Target: d.member, Cond: od.cond}
	_, done := od.attributed[executionKey{key, d.exec}]
	return done
}

// obligation is an obligation handed over, kept or revoked once the order is known.
type obligation struct {
	p       C.Proxy
	signals []time.Time
	dial    time.Time
}

func maxInt64(a, b int64) int64 {
	if b > a {
		return b
	}
	return a
}

// inherit merges the state of the group this one replaces with what this one observed since
// it was published.
func (od *onDemand) inherit(state inheritedState, now time.Time) {
	storeMax(&od.lastDial, state.lastDial)
	storeMax(&od.lastData, state.lastData)
	storeMax(&od.lastActive, state.lastActive)
	if state.lastSwitch.After(od.lastSwitch) {
		od.lastSwitch = state.lastSwitch
	}
	if state.repairUntil.After(od.repairUntil) {
		od.repairUntil = state.repairUntil
	}
	if state.dialed != nil {
		od.dialed.CompareAndSwap(nil, state.dialed)
	}
	if state.carried != nil {
		od.carried.CompareAndSwap(nil, state.carried)
	}
	od.inheritSummaries(state)
	od.inheritDialCounts(state.dialCounts)
	fields := od.event("inherit")
	sameCond := state.cond == od.cond
	fields["sameTest"] = sameCond
	if !sameCond {
		for p, inherited := range state.members {
			if len(inherited.signals) > 0 {
				od.recordRevoked(p, inherited.signals[0], "testChanged")
			}
		}
		if state.search != nil {
			fields["searchRevoked"] = state.search.id
		}
		if state.prefer != nil {
			for p, c := range state.prefer.confirms {
				od.recordConfirmEnd(p, c, "testChanged")
			}
		}
		od.record(fields)
		return
	}
	var obliged []obligation
	for p, inherited := range state.members {
		if !od.isMember(p) {
			if len(inherited.signals) > 0 {
				od.recordRevoked(p, inherited.signals[0], "memberRemoved")
			}
			continue
		}
		ms := od.member(p)
		ms.lastSuccessDial = maxTime(ms.lastSuccessDial, inherited.lastSuccessDial)
		if ms.lastDelay == 0 {
			ms.lastDelay = inherited.lastDelay
		}
		// The longer run of false alarms stands, unless this group saw it end.
		if ms.cooldownReset.IsZero() && inherited.cooldown > ms.cooldown {
			ms.cooldown, ms.cooldownUntil, ms.cooldownExec = inherited.cooldown, inherited.cooldownUntil, inherited.cooldownExec
		}
		if ms.cooldownExec == 0 {
			ms.cooldownExec = inherited.cooldownExec
		}
		ms.lastSignal = maxTime(ms.lastSignal, inherited.lastSignal)
		if len(inherited.signals) > 0 {
			obliged = append(obliged, obligation{p: p, signals: inherited.signals, dial: inherited.obligationDial})
		}
	}
	// What this group found itself since it was published is added to the standings handed
	// over, and the attributions it heard settle the anomalies the trials wait for.
	// The standings of the members the new group still has, under the same test, go on: the
	// round of a trial with its time and witness, the anomalies it waits for, the failed trials
	// and the readmission.
	inherited := 0
	for p, q := range state.quality {
		if !od.isMember(p) || od.unprobed(p) {
			continue
		}
		merged := q.clone()
		if own := od.quality[p]; own != nil {
			merged.absorb(own)
		}
		od.quality[p] = merged
		inherited++
	}
	for _, c := range od.early {
		od.trialCompletion(c)
	}
	for _, s := range od.earlySuccesses {
		if q := od.quality[s.member]; q != nil && q.trial != nil && !s.dialed.Before(q.trial.started) && !od.delegated(s.member) {
			q.trial.witness = true
		}
	}
	for _, a := range od.attributed {
		if q := od.quality[a.Key.Target]; q != nil {
			od.resolvePending(q, a.Attributed)
		}
	}
	for _, d := range state.degradations {
		if od.isMember(d.member) && !od.hasDegradation(d) {
			copied := *d
			od.degradations = append(od.degradations, &copied)
		}
	}
	if od.strict {
		// the order is the configured one: what made a member yield or go on trial is kept,
		// the standings are not
		for _, q := range od.quality {
			q.standing, q.trial, q.reason, q.readmitAt = standingNormal, nil, "", time.Time{}
		}
	}
	if od.inheritPrefer(state.prefer, now) {
		fields["preference"] = od.ref(od.prefer.preferred)
	}
	fields["standings"] = inherited
	od.computeOrder()
	if s := state.search; s != nil {
		if od.search == nil {
			od.search = &searchState{id: s.id, t0: s.t0, cause: s.cause}
		} else {
			// Keep the search of the old group, sure after both failures.
			own := od.search.id
			od.search = &searchState{id: s.id, t0: maxTime(s.t0, od.search.t0), cause: s.cause}
			fields["mergedSearch"] = own
		}
		fields["search"] = od.search.id
		fields["t0"] = formatTime(od.search.t0)
	} else if od.search != nil {
		fields["search"] = od.search.id
		fields["t0"] = formatTime(od.search.t0)
	}
	od.record(fields)
	od.reselect(now, selectionCause{Reason: causeReload})
	// The obligations go on for the preferred member, as reselected, and the standbys still
	// in range.
	refill, _ := od.refillRange()
	for _, o := range obliged {
		if o.p != od.current && !slices.Contains(refill, o.p) {
			od.recordRevoked(o.p, o.signals[0], "outOfRange")
			continue
		}
		ms := od.member(o.p)
		ms.signals = append(slices.Clone(o.signals), ms.signals...)
		slices.SortFunc(ms.signals, func(a, b time.Time) int { return a.Compare(b) })
		ms.obligationDial = maxTime(ms.obligationDial, o.dial)
		// A success either group saw explains signals of connections dialed before it.
		if ms.lastSuccessDial.After(ms.obligationDial) {
			fields := od.event("obligation")
			fields["member"] = od.ref(o.p)
			fields["end"] = "suppressed"
			fields["since"] = formatTime(ms.signals[0])
			fields["successDial"] = formatTime(ms.lastSuccessDial)
			od.recordAs("obligation", fields)
			ms.signals = nil
			ms.obligationDial = time.Time{}
		}
	}
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02T15:04:05.000Z07:00")
}

// ref names a proxy in a record. An object no longer in the config is recorded without a
// number rather than numbered again.
func (od *onDemand) ref(p C.Proxy) any {
	if p == nil {
		return nil
	}
	return map[string]any{"name": p.Name(), "object": od.coord.KnownObjectID(p)}
}

func (od *onDemand) refs(list []C.Proxy) []any {
	refs := make([]any, 0, len(list))
	for _, p := range list {
		refs = append(refs, od.ref(p))
	}
	return refs
}

func (od *onDemand) event(kind string) map[string]any {
	fields := map[string]any{"group": od.name(), "event": kind}
	if od.network != probe.NetworkNormal {
		fields["network"] = od.network
	}
	return fields
}

func (od *onDemand) recordFieldsFor(string) map[string]any {
	return map[string]any{"group": od.name()}
}

func (od *onDemand) record(fields map[string]any) {
	od.coord.Records().Append("group", fields)
}

func (od *onDemand) recordAs(kind string, fields map[string]any) {
	delete(fields, "event")
	od.coord.Records().Append(kind, fields)
}

func (od *onDemand) evidenceOf(p C.Proxy) map[string]any {
	if p == nil {
		return nil
	}
	ev := od.view[p]
	if od.unprobed(p) && !ev.Delegated {
		return map[string]any{"state": "builtin"}
	}
	fields := map[string]any{"state": ev.State}
	if ev.Delegated {
		fields["delegated"] = true
		if ev.Terminal {
			fields["terminal"] = true
		}
	}
	if !ev.Started.IsZero() {
		fields["probedAt"] = formatTime(ev.Started)
		fields["probe"] = ev.Exec
		fields["failures"] = ev.Failures
	}
	if !ev.NextAllowed.IsZero() {
		fields["nextAllowed"] = formatTime(ev.NextAllowed)
	}
	return fields
}

func (od *onDemand) memberRef(p C.Proxy) map[string]any {
	ref := map[string]any{"name": p.Name(), "object": od.coord.KnownObjectID(p)}
	for k, v := range od.evidenceOf(p) {
		ref[k] = v
	}
	return ref
}

// snapshotFields is the state of the group a need is recorded with.
func (od *onDemand) snapshotFields(now time.Time) map[string]any {
	fields := map[string]any{
		"inUse": map[string]any{
			"lastDial": formatUnix(od.lastDial.Load()),
			"lastData": formatUnix(od.lastData.Load()),
		},
		"pinned":      nilIfEmpty(od.pinned),
		"repairUntil": nilIfZero(od.repairUntil),
		"cooldown":    nil,
		"obligation":  nil,
		"search":      nil,
	}
	if od.current != nil {
		fields["current"] = od.memberRef(od.current)
		if ms := od.states[od.current]; ms != nil {
			if ms.cooldown != 0 {
				fields["cooldown"] = map[string]any{"cooldown": ms.cooldown.Milliseconds(), "until": formatTime(ms.cooldownUntil)}
			}
			if len(ms.signals) > 0 {
				fields["obligation"] = map[string]any{"since": formatTime(ms.signals[0]), "signals": len(ms.signals)}
			}
		}
	} else {
		fields["current"] = nil
	}
	pool := make([]any, 0, len(od.pool))
	for _, p := range od.pool {
		pool = append(pool, od.memberRef(p))
	}
	fields["pool"] = pool
	if len(od.forwarded) > 0 {
		// what the groups listing it need of it
		fields["forwarded"] = od.forwarded
	}
	if s := od.search; s != nil {
		var pending, confirmed []any
		for _, p := range od.searchable() {
			if od.confirmed(p) {
				confirmed = append(confirmed, map[string]any{"name": p.Name(), "object": od.coord.KnownObjectID(p), "probe": od.view[p].Exec})
			}
		}
		for _, p := range s.members {
			if od.isMember(p) && od.available(p) && !od.confirmed(p) {
				pending = append(pending, od.ref(p))
			}
		}
		fields["search"] = map[string]any{
			"id": s.id, "t0": formatTime(s.t0), "batch": s.batch, "pending": pending, "confirmed": confirmed,
		}
	}
	return fields
}

func formatUnix(nanos int64) any {
	if nanos == 0 {
		return nil
	}
	return formatTime(time.Unix(0, nanos))
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nilIfZero(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return formatTime(t)
}

// State returns the state of the on-demand groups of the config in use.
func State() []any {
	lifecycle.Lock()
	groups := make([]*onDemand, 0, len(lifecycle.groups))
	for _, od := range lifecycle.groups {
		groups = append(groups, od)
	}
	lifecycle.Unlock()
	sort.Slice(groups, func(i, j int) bool { return groups[i].name() < groups[j].name() })
	list := make([]any, 0, len(groups))
	for _, od := range groups {
		list = append(list, od.state())
	}
	return list
}

func (od *onDemand) state() map[string]any {
	od.mutex.Lock()
	defer od.mutex.Unlock()
	now := od.now()
	refill, pool := od.refillRange()
	members := make([]any, 0, len(od.order))
	for i, p := range od.order {
		entry := od.memberRef(p)
		entry["position"] = i
		entry["excluded"] = od.excluded(p)
		if !od.unprobed(p) {
			entry["standing"] = od.standingOf(p).String()
			entry["counts"] = od.countsOf(p, now)
			entry["trusted"] = od.trusted(p, now)
			entry["suspended"] = od.suspended != nil && od.suspended.member == p
			if q := od.quality[p]; q != nil {
				entry["readmitAt"] = nilIfZero(q.readmitAt)
				entry["failedTrials"] = q.failedTrials
				if t := q.trial; t != nil {
					entry["trial"] = map[string]any{"round": t.round, "reason": t.reason, "started": formatTime(t.started), "effective": t.effective.Milliseconds(), "witness": t.witness, "pending": len(t.pending), "paused": t.resumed.IsZero()}
				}
			}
		}
		if ms := od.states[p]; ms != nil {
			entry["lastSuccessDial"] = nilIfZero(ms.lastSuccessDial)
			if ms.cooldown != 0 {
				entry["cooldown"] = map[string]any{"cooldown": ms.cooldown.Milliseconds(), "until": formatTime(ms.cooldownUntil)}
			}
			if len(ms.signals) > 0 {
				entry["obligation"] = map[string]any{"since": formatTime(ms.signals[0]), "signals": len(ms.signals)}
			}
		}
		members = append(members, entry)
	}
	state := map[string]any{
		"name":           od.name(),
		"test":           map[string]any{"url": od.cond.URL, "expectedStatus": od.cond.Expected, "timeout": od.cond.Timeout.Milliseconds()},
		"inUse":          od.inUseLocked(now),
		"lastDial":       formatUnix(od.lastDial.Load()),
		"lastData":       formatUnix(od.lastData.Load()),
		"repairUntil":    nilIfZero(od.repairUntil),
		"pinned":         nilIfEmpty(od.pinned),
		"current":        od.ref(od.current),
		"preferred":      od.ref(od.current),
		"route":          od.ref(od.route),
		"network":        od.network,
		"refill":         od.refs(refill),
		"pool":           od.refs(pool),
		"members":        members,
		"active":         od.reg != nil && od.reg.Active(),
		"selectionCause": od.cause,
	}
	if s := od.search; s != nil {
		state["search"] = map[string]any{"id": s.id, "t0": formatTime(s.t0), "batch": s.batch, "pending": od.refs(s.members)}
	}
	return state
}

// TrafficPools counts the nodes of each traffic pool in the config in use, the members of
// its providers included.
func TrafficPools(proxies map[string]C.Proxy, providers map[string]P.ProxyProvider) map[string]int {
	counts := map[string]int{}
	seen := map[C.Proxy]struct{}{}
	count := func(p C.Proxy) {
		if _, ok := seen[p]; ok {
			return
		}
		seen[p] = struct{}{}
		if owner, ok := p.(C.TrafficPoolOwner); ok && owner.TrafficPool() != "" {
			counts[owner.TrafficPool()]++
		}
	}
	for _, p := range proxies {
		count(p)
	}
	for _, pd := range providers {
		for _, p := range pd.Proxies() {
			count(p)
		}
	}
	return counts
}
