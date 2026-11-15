package outboundgroup

import (
	"time"

	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"
)

// A member that is an on-demand group under the same condition, reaching no outbound that
// controls the rules, judges its own availability: the group reads what it publishes and
// forwards it what it needs, instead of testing through it. A test through it would take
// the path it selects at that moment, which may be one it is just leaving.

// publication is what a group last published of its availability.
type publication struct {
	availability probe.Availability
	preferred    C.Proxy
}

// delegated reports whether a member judges its own availability.
func (od *onDemand) delegated(p C.Proxy) bool {
	return od.view[p].Delegated
}

// delegateOf is the group of a member that judges its own availability, nil for any other.
func (od *onDemand) delegateOf(p C.Proxy) *onDemand {
	if p == nil || !od.delegated(p) {
		return nil
	}
	return onDemandOf(p)
}

// isDelegable reports whether the groups listing this one under its condition may take its
// availability from it: none of the outbounds it reaches through its members, whatever the
// groups on the way select, controls the rules or serves a purpose of its own.
func (od *onDemand) isDelegable() bool {
	seen := map[C.Proxy]bool{}
	var walk func(p C.Proxy) bool
	walk = func(p C.Proxy) bool {
		if seen[p] {
			return true
		}
		seen[p] = true
		switch p.Type() {
		case C.Pass, C.PassRule, C.Rematch, C.Dns:
			return false
		}
		if group, ok := p.Adapter().(ProxyGroup); ok {
			for _, member := range group.Proxies() {
				if !walk(member) {
					return false
				}
			}
		}
		return true
	}
	for _, p := range od.f.GetProxies(false) {
		if !walk(p) {
			return false
		}
	}
	return true
}

// terminal reports whether a member, as the group selects it, ends at a direct outbound
// that is not tested: always there, never confirmed by a test.
func (od *onDemand) terminal(p C.Proxy) bool {
	if od.view[p].Terminal {
		return true
	}
	return probe.IsBuiltin(p) && !od.probed[p] && (p.Type() == C.Direct || p.Type() == C.Compatible)
}

// availability is what the group judges of itself, by the member it prefers: available
// through a tested path, at a direct outbound it does not test, or not at all.
func (od *onDemand) availability() (probe.Availability, C.Proxy) {
	cur := od.current
	var a probe.Availability
	switch {
	case cur == nil:
		a.State = probe.Dead
	case od.terminal(cur):
		a.State, a.Terminal = probe.Alive, true
	case od.unprobed(cur):
		a.State = probe.Dead // a reject outbound
	default:
		ev := od.view[cur]
		a.State, a.Probe = ev.State, cur
		if ev.State == probe.Alive {
			a.Proof, a.Exec = ev.Started, ev.Exec
			if ms := od.states[cur]; ms != nil {
				a.Delay = ms.lastDelay
			}
		}
	}
	if a.State == probe.Dead {
		switch od.cause.Reason {
		case causeVerify, causeContinue, causeSearch, causeExternal, causeProbe, causeDelegate:
			a.Failed = true
		}
		// the latest test that found a member before it dead; none when the members are
		// excluded, or a reject outbound is pinned
		for _, p := range od.order {
			if p == cur || od.unprobed(p) {
				break
			}
			if ev := od.view[p]; !od.excluded(p) && ev.State == probe.Dead {
				if a.Probe == nil {
					a.Probe = p // the first to recover
				}
				if ev.Started.After(a.Proof) {
					a.Proof, a.Exec = ev.Started, ev.Exec
				}
			}
		}
	}
	return a, cur
}

// delegationLocked decides whether the group judges its own availability, as eligible says,
// and what it publishes of it when that changed: change is set when the decision changed,
// next when there is something to publish.
func (od *onDemand) delegationLocked(eligible bool) (change *bool, next *probe.Availability) {
	if od.self == nil {
		return nil, nil
	}
	if eligible != od.delegating {
		od.delegating = eligible
		od.published.Store(nil)
		change = &eligible
		fields := od.event("delegate")
		fields["delegated"] = eligible
		od.record(fields)
	}
	if !od.delegating {
		return change, nil
	}
	a, preferred := od.availability()
	if last := od.published.Load(); last != nil && last.preferred == preferred && last.availability.State == a.State &&
		last.availability.Terminal == a.Terminal && last.availability.Proof.Equal(a.Proof) && last.availability.Probe == a.Probe {
		return change, nil
	}
	fields := od.event("availability")
	fields["state"] = a.State
	if a.Terminal {
		fields["terminal"] = true
	}
	fields["preferred"] = od.ref(preferred)
	if !a.Proof.IsZero() {
		fields["proof"] = formatTime(a.Proof)
		fields["probe"] = a.Exec
	}
	fields["cause"] = od.routeCause
	a.Seq = od.coord.Records().Append("group", fields)
	od.published.Store(&publication{availability: a, preferred: preferred})
	return change, &a
}

// delegatedBadge tells that a member judges its own availability, with the member it prefers.
func (od *onDemand) delegatedBadge(p C.Proxy) map[string]any {
	text := probe.T("由子组判定", "judged by itself")
	if child := onDemandOf(p); child != nil {
		if last := child.published.Load(); last != nil && last.preferred != nil {
			name := last.preferred.Name()
			text = probe.Text{"zh": "由子组判定 · 首选 " + name, "en": "judged by itself · prefers " + name}
		}
	}
	return badgeTip("delegated", probe.LevelInfo, text, probe.T(
		"测试条件与本组相同的按需组：可用性由它自己按其路径判定，本组不经它探测",
		"an on-demand group under the same test: it judges its availability by its paths itself, and this group does not test through it"))
}

// settleNote asks the group to take in what it was told before it, done closed once it has
// and published what follows.
type settleNote struct {
	done chan struct{}
}

// settleGroups lets each group take in what it was told, in order, each waiting a while at
// most: listed groups first, the groups listing them read what they published then.
func settleGroups(groups []*onDemand, wait time.Duration) {
	for _, od := range groups {
		done := make(chan struct{})
		od.post(settleNote{done: done})
		select {
		case <-done:
		case <-time.After(wait):
		}
	}
}

// forwardedNeeds adds what the groups listing this one forwarded: a confirmation of the
// path it prefers while it is available, a recovery while it is not. It reports whether a
// recovery is asked for, and until when when only repair windows ask for it.
func (od *onDemand) forwardedNeeds(add func(p C.Proxy, class probe.Class, notBefore time.Time)) (recover bool, repairUntil time.Time) {
	cur := od.current
	unavailable := cur == nil || (od.unprobed(cur) && !od.terminal(cur))
	plain := false
	for _, f := range od.forwarded {
		if unavailable {
			if f.NotBefore.IsZero() {
				recover = true
				if f.RepairUntil.IsZero() {
					plain = true
				} else if f.RepairUntil.After(repairUntil) {
					repairUntil = f.RepairUntil
				}
			}
			continue
		}
		// a direct outbound it does not test ends the confirmation by what it published
		if !od.unprobed(cur) {
			add(cur, f.Class, f.NotBefore)
		}
	}
	if plain {
		repairUntil = time.Time{}
	}
	return recover, repairUntil
}
