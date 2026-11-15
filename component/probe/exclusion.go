package probe

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// ExclusionEntry excludes the nodes of a traffic pool until a time, or for good when Until is
// zero.
type ExclusionEntry struct {
	Pool  string    `json:"pool"`
	Until time.Time `json:"until,omitzero"`
}

// Table is a version of the exclusion table: the pools it excludes. It is not changed once
// made.
type Table struct {
	Pools   map[string]struct{}
	Version uint64
}

// Exclusions is the table of traffic pools new routes avoid. It lives in memory: it survives
// reloads and is empty after a restart.
type Exclusions struct {
	mutex   sync.Mutex
	entries map[string]time.Time
	timer   *time.Timer
	now     func() time.Time
	// version numbers the states of the table; it is 1 for the empty table it starts as.
	version uint64
	// table is the current version, read without the lock.
	table atomic.Pointer[Table]
	// commit makes a version current, with the lock held: publish stores the version, and
	// commit runs it along with what else must change at the same time, such as the records
	// and the notices of the groups, in the order of the versions.
	commit func(entries []ExclusionEntry, version uint64, publish func())
	// changed is called, without the lock held, after the table changed.
	changed func(table Table)
}

func newExclusions(now func() time.Time, commit func([]ExclusionEntry, uint64, func()), changed func(Table)) *Exclusions {
	e := &Exclusions{entries: map[string]time.Time{}, now: now, commit: commit, changed: changed, version: 1}
	e.table.Store(&Table{Pools: map[string]struct{}{}, Version: 1})
	return e
}

// Current returns the current version of the table.
func (e *Exclusions) Current() Table {
	return *e.table.Load()
}

// publishLocked makes the entries the current version of the table.
func (e *Exclusions) publishLocked() Table {
	e.version++
	pools := make(map[string]struct{}, len(e.entries))
	for pool := range e.entries {
		pools[pool] = struct{}{}
	}
	table := Table{Pools: pools, Version: e.version}
	e.commit(e.listLocked(), e.version, func() { e.table.Store(&table) })
	return table
}

// Replace replaces the whole table. Entries that already expired are dropped.
func (e *Exclusions) Replace(entries []ExclusionEntry) {
	e.mutex.Lock()
	now := e.now()
	e.entries = map[string]time.Time{}
	for _, entry := range entries {
		if entry.Pool == "" || (!entry.Until.IsZero() && !entry.Until.After(now)) {
			continue
		}
		e.entries[entry.Pool] = entry.Until
	}
	table := e.publishLocked()
	e.scheduleLocked()
	e.mutex.Unlock()
	e.changed(table)
}

// List returns the table, sorted by pool.
func (e *Exclusions) List() []ExclusionEntry {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	return e.listLocked()
}

func (e *Exclusions) listLocked() []ExclusionEntry {
	list := make([]ExclusionEntry, 0, len(e.entries))
	for pool, until := range e.entries {
		list = append(list, ExclusionEntry{Pool: pool, Until: until})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Pool < list[j].Pool })
	return list
}

func (e *Exclusions) scheduleLocked() {
	if e.timer != nil {
		e.timer.Stop()
		e.timer = nil
	}
	var earliest time.Time
	for _, until := range e.entries {
		if !until.IsZero() && (earliest.IsZero() || until.Before(earliest)) {
			earliest = until
		}
	}
	if earliest.IsZero() {
		return
	}
	e.timer = time.AfterFunc(earliest.Sub(e.now()), e.expire)
}

func (e *Exclusions) expire() {
	e.mutex.Lock()
	now := e.now()
	expired := false
	for pool, until := range e.entries {
		if !until.IsZero() && !until.After(now) {
			delete(e.entries, pool)
			expired = true
		}
	}
	var table Table
	if expired {
		table = e.publishLocked()
	}
	e.scheduleLocked()
	e.mutex.Unlock()
	if expired {
		e.changed(table)
	}
}

// Empty reports whether no pool is excluded; nothing needs evaluating then.
func (e *Exclusions) Empty() bool {
	return len(e.table.Load().Pools) == 0
}

// TraceStep is one step of an exclusion evaluation, recorded as the reason of a result.
type TraceStep struct {
	Name     string `json:"name"`
	Excluded bool   `json:"excluded"`
	// By tells what decided the step: "pool", "dialer", "selection", "members", "member",
	// "cycle", "builtin" or "none".
	By     string `json:"by"`
	Detail string `json:"detail,omitempty"`
}

// groupView is what the evaluation reads of a group.
type groupView interface {
	Proxies() []C.Proxy
}

// onDemand is implemented by groups that avoid excluded members themselves.
type onDemand interface {
	OnDemand() bool
}

// IsBuiltin reports whether a proxy is a built-in outbound, which is never excluded or
// probed.
func IsBuiltin(p C.Proxy) bool {
	switch p.Type() {
	case C.Direct, C.Reject, C.RejectDrop, C.Compatible, C.Pass, C.PassRule, C.Rematch, C.Dns:
		return true
	}
	return false
}

// Evaluator evaluates exclusions against the table as it was when the evaluator was made. It
// reads the current selection of the groups that do not avoid excluded members, so it is
// made at each point a decision needs it rather than cached across such points. Nothing is
// cached between evaluations: what a member is found to be depends on the path it was
// reached by when the path closes a cycle, and every result carries all of its reasons.
type Evaluator struct {
	pools   map[string]struct{}
	version uint64
	lookup  func(name string) C.Proxy
	path    map[C.Proxy]bool
	trace   []TraceStep
}

// NewEvaluator returns an evaluator of the current table; lookup finds proxies and groups by
// name, as dialer-proxy names them.
func (e *Exclusions) NewEvaluator(lookup func(name string) C.Proxy) *Evaluator {
	return NewEvaluatorFor(e.Current(), lookup)
}

// NewEvaluatorFor returns an evaluator of a version of the table.
func NewEvaluatorFor(table Table, lookup func(name string) C.Proxy) *Evaluator {
	return &Evaluator{pools: table.Pools, version: table.Version, lookup: lookup, path: map[C.Proxy]bool{}}
}

// Version is the version of the table the evaluator evaluates against.
func (ev *Evaluator) Version() uint64 {
	return ev.version
}

// Excluded reports whether p is excluded, with the steps that decided it.
func (ev *Evaluator) Excluded(p C.Proxy) (bool, []TraceStep) {
	ev.trace = nil
	excluded := ev.evaluate(p)
	return excluded, ev.trace
}

func (ev *Evaluator) step(name string, excluded bool, by, detail string) bool {
	ev.trace = append(ev.trace, TraceStep{Name: name, Excluded: excluded, By: by, Detail: detail})
	return excluded
}

func (ev *Evaluator) evaluate(p C.Proxy) bool {
	if p == nil {
		return false
	}
	if ev.path[p] {
		// A cycle through members and dialer proxies cannot carry traffic anyway.
		return ev.step(p.Name(), true, "cycle", "")
	}
	ev.path[p] = true
	excluded := ev.evaluateOnPath(p)
	delete(ev.path, p)
	return excluded
}

func (ev *Evaluator) evaluateOnPath(p C.Proxy) bool {
	if IsBuiltin(p) {
		return ev.step(p.Name(), false, "builtin", "")
	}
	if group, ok := p.Adapter().(groupView); ok {
		return ev.evaluateGroup(p, group)
	}
	if owner, ok := p.(C.TrafficPoolOwner); ok {
		if pool := owner.TrafficPool(); pool != "" {
			if _, excluded := ev.pools[pool]; excluded {
				return ev.step(p.Name(), true, "pool", pool)
			}
		}
	}
	if dialer := p.ProxyInfo().DialerProxy; dialer != "" {
		if target := ev.lookup(dialer); target != nil && ev.evaluate(target) {
			return ev.step(p.Name(), true, "dialer", dialer)
		}
	}
	return ev.step(p.Name(), false, "none", "")
}

func (ev *Evaluator) evaluateGroup(p C.Proxy, group groupView) bool {
	members := group.Proxies()
	if od, ok := p.Adapter().(onDemand); ok && od.OnDemand() {
		// It avoids excluded members itself: excluded only when none is left.
		any := false
		for _, member := range members {
			if IsBuiltin(member) {
				continue
			}
			any = true
			if !ev.evaluate(member) {
				return ev.step(p.Name(), false, "members", member.Name())
			}
		}
		return ev.step(p.Name(), any, "members", "")
	}
	if p.Type() == C.LoadBalance {
		// Each connection picks its own member: any excluded member may be picked.
		for _, member := range members {
			if !IsBuiltin(member) && ev.evaluate(member) {
				return ev.step(p.Name(), true, "member", member.Name())
			}
		}
		return ev.step(p.Name(), false, "members", "")
	}
	// A group with a stable selection is judged by the member it selects now, taken as the
	// object it routes through rather than by name: a provider may replace the members
	// between two reads.
	selected := p.Unwrap(nil, false)
	if selected == nil {
		return ev.step(p.Name(), false, "selection", "")
	}
	excluded := ev.evaluate(selected)
	return ev.step(p.Name(), excluded, "selection", selected.Name())
}
