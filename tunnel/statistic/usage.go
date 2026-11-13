package statistic

import (
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
)

const (
	// UsageRetainHours is how many hours before the current one the per-proxy usage keeps.
	UsageRetainHours = 31 * 24
	// UsageOverflowName names the entry that takes the traffic of proxies beyond UsageMaxEntries.
	UsageOverflowName = "*overflow*"
	// UsageMaxEntries bounds the number of proxies tracked individually.
	UsageMaxEntries = 10000
)

// DefaultUsage records the traffic of every tracked connection per outbound proxy and hour.
var DefaultUsage *Usage

func init() {
	DefaultUsage = newUsage(time.Now)
	go DefaultUsage.run()
}

// OutboundIdentity is implemented by the connections outbound adapters build.
type OutboundIdentity interface {
	OutboundIdentity() (adapterType C.AdapterType, addr string)
}

// UsageKey identifies the proxy a connection's traffic is credited to.
type UsageKey struct {
	Provider string
	Name     string
	Type     string
	Addr     string
	// Incomplete is set when the connection did not reveal the adapter that made it.
	Incomplete bool
}

func usageKeyOf(conn any, chain, providerChain C.Chain) UsageKey {
	var key UsageKey
	if len(chain) > 0 {
		key.Name = chain[0]
	}
	if len(providerChain) > 0 {
		key.Provider = providerChain[0]
	}
	// The first identity met is the proxy this connection was made by; going further down
	// would reach the connection of a dialer proxy it was relayed through.
	if identity, ok := N.FindUpstream[OutboundIdentity](conn, nil); ok {
		adapterType, addr := identity.OutboundIdentity()
		key.Type = adapterType.String()
		key.Addr = addr
	} else {
		key.Incomplete = true
	}
	return key
}

type usageBucket struct {
	hour     int64
	up       atomic.Int64
	down     atomic.Int64
	sealed   atomic.Bool
	inflight atomic.Int32
}

type usageEntry struct {
	usage *Usage
	key   UsageKey
	refs  int64 // guarded by usage.mu

	current atomic.Pointer[usageBucket]

	mu       sync.Mutex
	buckets  []*usageBucket // ascending by hour
	lastHour int64          // hour of the latest bucket
	detached bool           // removed from the registry
}

func (e *usageEntry) add(up, down int64) {
	if up == 0 && down == 0 {
		return
	}
	for {
		b := e.current.Load()
		if b == nil || b.hour != e.usage.hour.Load() {
			if b = e.rotate(); b == nil {
				// A write that outlived its tracker, after the idle entry was removed.
				e.usage.entryOf(e.key).add(up, down)
				return
			}
		}
		b.inflight.Add(1)
		if b.sealed.Load() {
			// Sealed after the clock moved on: the write belongs to the current hour.
			b.inflight.Add(-1)
			continue
		}
		b.up.Add(up)
		b.down.Add(down)
		b.inflight.Add(-1)
		return
	}
}

// rotate returns the bucket of the current hour, creating it if needed, or nil once the
// entry is detached. Reading the clock under the entry lock keeps a bucket for an hour
// already sealed from being created.
func (e *usageEntry) rotate() *usageBucket {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.detached {
		return nil
	}
	hour := e.usage.hour.Load()
	if n := len(e.buckets); n > 0 && e.buckets[n-1].hour == hour {
		b := e.buckets[n-1]
		e.current.Store(b)
		return b
	}
	b := &usageBucket{hour: hour}
	e.buckets = append(e.buckets, b)
	e.lastHour = hour
	e.current.Store(b)
	return b
}

// Usage counts traffic per outbound proxy in UTC hours.
//
// Hours before the current one are sealed once the clock passes them, and are final from
// then on; settled is the latest such hour. Hours before retainedSince are dropped.
type Usage struct {
	now       func() time.Time
	instance  string
	startedAt time.Time

	hour          atomic.Int64 // never goes back, even when the system clock does
	settled       atomic.Int64
	retainedSince atomic.Int64

	mu       sync.Mutex
	entries  map[UsageKey]*usageEntry
	overflow *usageEntry

	// snapshotMu keeps expiry from changing the retained range while a snapshot is copied.
	snapshotMu sync.RWMutex
}

func newUsage(now func() time.Time) *Usage {
	startedAt := now()
	hour := unixHour(startedAt)
	u := &Usage{
		now:       now,
		instance:  utils.NewUUIDV4().String(),
		startedAt: startedAt,
		entries:   make(map[UsageKey]*usageEntry),
	}
	u.overflow = &usageEntry{usage: u, key: UsageKey{Name: UsageOverflowName}}
	u.hour.Store(hour)
	u.settled.Store(hour - 1)
	u.retainedSince.Store(hour)
	return u
}

func unixHour(t time.Time) int64 {
	return t.Unix() / 3600
}

func (u *Usage) run() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		u.tick()
	}
}

func (u *Usage) tick() {
	hour := unixHour(u.now())
	if hour <= u.hour.Load() {
		return
	}
	u.hour.Store(hour)
	u.seal(hour)
	u.expire(hour)
}

func (u *Usage) acquire(key UsageKey) *usageEntry {
	u.mu.Lock()
	defer u.mu.Unlock()
	e := u.lookup(key)
	if e != u.overflow {
		e.refs++
	}
	return e
}

// entryOf returns the registered entry of key without taking a reference.
func (u *Usage) entryOf(key UsageKey) *usageEntry {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.lookup(key)
}

func (u *Usage) lookup(key UsageKey) *usageEntry {
	if e, ok := u.entries[key]; ok {
		return e
	}
	if len(u.entries) >= UsageMaxEntries {
		return u.overflow
	}
	e := &usageEntry{usage: u, key: key}
	u.entries[key] = e
	return e
}

func (u *Usage) release(e *usageEntry) {
	if e == u.overflow {
		return
	}
	u.mu.Lock()
	e.refs--
	u.mu.Unlock()
}

func (u *Usage) allEntries() []*usageEntry {
	u.mu.Lock()
	defer u.mu.Unlock()
	entries := make([]*usageEntry, 0, len(u.entries)+1)
	for _, e := range u.entries {
		entries = append(entries, e)
	}
	return append(entries, u.overflow)
}

// seal marks the buckets of hours before hour as sealed, waits for the writes already in
// them to finish, and then publishes hour-1 as settled. The clock has moved to hour before
// this runs, so new buckets are only made for hour or later.
func (u *Usage) seal(hour int64) {
	var pending []*usageBucket
	for _, e := range u.allEntries() {
		e.mu.Lock()
		for _, b := range e.buckets {
			if b.hour < hour && !b.sealed.Load() {
				b.sealed.Store(true)
				pending = append(pending, b)
			}
		}
		e.mu.Unlock()
	}
	for _, b := range pending {
		for b.inflight.Load() != 0 {
			runtime.Gosched()
		}
	}
	u.settled.Store(hour - 1)
}

// expire drops the hours no longer kept and removes the entries left idle.
func (u *Usage) expire(hour int64) {
	u.snapshotMu.Lock()
	defer u.snapshotMu.Unlock()
	retainedSince := u.retainedSince.Load()
	if hour-UsageRetainHours > retainedSince {
		retainedSince = hour - UsageRetainHours
		u.retainedSince.Store(retainedSince)
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	for key, e := range u.entries {
		if e.dropBefore(retainedSince, hour, true) {
			delete(u.entries, key)
		}
	}
	u.overflow.dropBefore(retainedSince, hour, false)
}

// dropBefore removes the buckets before retainedSince. With detach, an entry no tracker
// refers to, left without buckets and not written in the last hour is marked detached, in
// the same critical section, and true is returned for it to be removed from the registry.
// The caller holds u.mu, which guards refs.
func (e *usageEntry) dropBefore(retainedSince, hour int64, detach bool) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	i := 0
	for i < len(e.buckets) && e.buckets[i].hour < retainedSince {
		i++
	}
	if i > 0 {
		e.buckets = slices.Delete(e.buckets, 0, i)
		if b := e.current.Load(); b != nil && b.hour < retainedSince {
			e.current.Store(nil)
		}
	}
	if detach && e.refs == 0 && len(e.buckets) == 0 && e.lastHour < hour-1 {
		e.detached = true
	}
	return e.detached
}

// UsageBucket is the traffic of one proxy in one hour.
type UsageBucket struct {
	Hour int64 `json:"hour"`
	Up   int64 `json:"up"`
	Down int64 `json:"down"`
}

// UsageEntry is the traffic of one proxy.
type UsageEntry struct {
	Provider   string        `json:"provider"`
	Name       string        `json:"name"`
	Type       string        `json:"type"`
	Addr       string        `json:"addr"`
	Incomplete bool          `json:"incomplete"`
	Buckets    []UsageBucket `json:"buckets"`
}

// UsageSnapshot is the per-proxy usage of a range of hours.
//
// Hours from RetainedSince to Settled are final. Later hours may still grow.
// Hours before RetainedSince are no longer kept: their absence says nothing about usage.
type UsageSnapshot struct {
	Instance      string       `json:"instance"`
	StartedAt     time.Time    `json:"startedAt"`
	Hour          int64        `json:"hour"`
	Settled       int64        `json:"settled"`
	RetainedSince int64        `json:"retainedSince"`
	Overflow      bool         `json:"overflow"`
	Entries       []UsageEntry `json:"entries"`
}

// Snapshot returns the non-zero buckets of hours from since to until, both inclusive.
func (u *Usage) Snapshot(since, until int64) *UsageSnapshot {
	u.snapshotMu.RLock()
	snapshot := &UsageSnapshot{
		Instance:      u.instance,
		StartedAt:     u.startedAt,
		RetainedSince: u.retainedSince.Load(),
		Settled:       u.settled.Load(),
		Hour:          u.hour.Load(),
		Entries:       []UsageEntry{},
	}
	for _, e := range u.allEntries() {
		buckets := e.collect(since, until)
		if len(buckets) == 0 {
			continue
		}
		if e == u.overflow {
			snapshot.Overflow = true
		}
		snapshot.Entries = append(snapshot.Entries, UsageEntry{
			Provider:   e.key.Provider,
			Name:       e.key.Name,
			Type:       e.key.Type,
			Addr:       e.key.Addr,
			Incomplete: e.key.Incomplete,
			Buckets:    buckets,
		})
	}
	u.snapshotMu.RUnlock()

	slices.SortFunc(snapshot.Entries, func(a, b UsageEntry) int {
		for _, order := range [...]int{
			strings.Compare(a.Provider, b.Provider),
			strings.Compare(a.Name, b.Name),
			strings.Compare(a.Type, b.Type),
			strings.Compare(a.Addr, b.Addr),
		} {
			if order != 0 {
				return order
			}
		}
		return 0
	})
	return snapshot
}

func (e *usageEntry) collect(since, until int64) []UsageBucket {
	e.mu.Lock()
	defer e.mu.Unlock()
	var buckets []UsageBucket
	for _, b := range e.buckets {
		if b.hour < since || b.hour > until {
			continue
		}
		up, down := b.up.Load(), b.down.Load()
		if up == 0 && down == 0 {
			continue
		}
		buckets = append(buckets, UsageBucket{Hour: b.hour, Up: up, Down: down})
	}
	return buckets
}

// usageCounter credits a tracker's traffic to the proxy that made its connection.
type usageCounter struct {
	entry   *usageEntry
	release sync.Once
}

func (c *usageCounter) add(up, down int64) {
	if c != nil {
		c.entry.add(up, down)
	}
}

func (c *usageCounter) leave() {
	if c != nil {
		c.release.Do(func() {
			c.entry.usage.release(c.entry)
		})
	}
}

// WithoutUsage keeps the tracker out of the per-proxy usage, for connections whose traffic
// is already counted on the connections they carry.
func WithoutUsage() TrackerOption {
	return func(info *TrackerInfo) {
		info.withoutUsage = true
	}
}

func (info *TrackerInfo) joinUsage(conn any, upload, download int64) {
	if info.withoutUsage {
		return
	}
	info.usage = &usageCounter{entry: DefaultUsage.acquire(usageKeyOf(conn, info.Chain, info.ProviderChain))}
	info.usage.add(upload, download)
}
