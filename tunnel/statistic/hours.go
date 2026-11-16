package statistic

import (
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// hourBucket is what one entry counted in one UTC hour.
type hourBucket struct {
	hour     int64
	up       atomic.Int64
	down     atomic.Int64
	conns    atomic.Int64
	sealed   atomic.Bool
	inflight atomic.Int32
}

type hourEntry[K comparable] struct {
	table *hourTable[K]
	key   K
	refs  int64 // guarded by table.mu

	current atomic.Pointer[hourBucket]
	// touched is the table's tick of the latest count, stored after the count is added.
	touched atomic.Int64

	mu       sync.Mutex
	buckets  []*hourBucket // ascending by hour
	lastHour int64         // hour of the latest bucket
	detached bool          // removed from the registry
}

func (e *hourEntry[K]) add(up, down, conns int64) {
	if up == 0 && down == 0 && conns == 0 {
		return
	}
	for {
		b := e.current.Load()
		if b == nil || b.hour != e.table.hour.Load() {
			if b = e.rotate(); b == nil {
				// A count that outlived its tracker, after the idle entry was removed.
				e.table.entryOf(e.key).add(up, down, conns)
				return
			}
		}
		b.inflight.Add(1)
		if b.sealed.Load() {
			// Sealed after the clock moved on: the count belongs to the current hour.
			b.inflight.Add(-1)
			continue
		}
		if admit := e.table.admit; admit != nil && !admit() {
			// Stopped after the caller checked: drain waits only for the counts admitted.
			b.inflight.Add(-1)
			return
		}
		b.up.Add(up)
		b.down.Add(down)
		if conns != 0 {
			b.conns.Add(conns)
		}
		b.inflight.Add(-1)
		e.touch()
		return
	}
}

// touch records the current tick as that of the entry's latest count, never moving it back.
func (e *hourEntry[K]) touch() {
	tick := e.table.ticks.Load()
	for {
		touched := e.touched.Load()
		if touched >= tick || e.touched.CompareAndSwap(touched, tick) {
			return
		}
	}
}

// rotate returns the bucket of the current hour, creating it if needed, or nil once the
// entry is detached. Reading the clock under the entry lock keeps a bucket for an hour
// already sealed from being created.
func (e *hourEntry[K]) rotate() *hourBucket {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.detached {
		return nil
	}
	hour := e.table.hour.Load()
	if n := len(e.buckets); n > 0 && e.buckets[n-1].hour == hour {
		b := e.buckets[n-1]
		e.current.Store(b)
		return b
	}
	b := &hourBucket{hour: hour}
	e.buckets = append(e.buckets, b)
	e.lastHour = hour
	e.current.Store(b)
	return b
}

// hourCount is a copy of a bucket.
type hourCount struct {
	hour, up, down, conns int64
}

// collect copies the non-empty buckets of hours from since to until, both inclusive.
func (e *hourEntry[K]) collect(since, until int64) []hourCount {
	e.mu.Lock()
	defer e.mu.Unlock()
	var counts []hourCount
	for _, b := range e.buckets {
		if b.hour < since || b.hour > until {
			continue
		}
		up, down, conns := b.up.Load(), b.down.Load(), b.conns.Load()
		if up == 0 && down == 0 && conns == 0 {
			continue
		}
		counts = append(counts, hourCount{hour: b.hour, up: up, down: down, conns: conns})
	}
	return counts
}

// dropBefore removes the buckets before retainedSince. With detach, an entry no tracker
// refers to, left without buckets and not counted in the last hour is marked detached, in
// the same critical section, and true is returned for it to be removed from the registry.
// The caller holds table.mu, which guards refs.
func (e *hourEntry[K]) dropBefore(retainedSince, hour int64, detach bool) bool {
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

// hourTable counts per key in UTC hours.
//
// Hours before the current one are sealed once the clock passes them, and are final from
// then on; settled is the latest such hour. Hours before retainedSince are dropped. Keys
// beyond maxEntries are counted on the overflow entry overflowIndex picks for them.
type hourTable[K comparable] struct {
	now           func() time.Time
	retainHours   int64
	maxEntries    int
	overflowIndex func(K) int
	// admit, when set, is checked by a count once in its bucket; see drain.
	admit func() bool

	hour          atomic.Int64 // never goes back, even when the system clock does
	settled       atomic.Int64
	retainedSince atomic.Int64
	// ticks counts the seconds the table's clock has run, never going back.
	ticks atomic.Int64

	mu       sync.Mutex
	entries  map[K]*hourEntry[K]
	overflow []*hourEntry[K]

	// snapshotMu keeps expiry from changing the retained range while a snapshot is copied.
	snapshotMu sync.RWMutex
}

func newHourTable[K comparable](now func() time.Time, retainHours int64, maxEntries int, overflowKeys []K, overflowIndex func(K) int) *hourTable[K] {
	hour := unixHour(now())
	t := &hourTable[K]{
		now:           now,
		retainHours:   retainHours,
		maxEntries:    maxEntries,
		overflowIndex: overflowIndex,
		entries:       make(map[K]*hourEntry[K]),
	}
	for _, key := range overflowKeys {
		t.overflow = append(t.overflow, &hourEntry[K]{table: t, key: key})
	}
	t.hour.Store(hour)
	t.settled.Store(hour - 1)
	t.retainedSince.Store(hour)
	return t
}

func unixHour(t time.Time) int64 {
	return t.Unix() / 3600
}

// tick advances the clock by a second, and when the hour has changed seals and expires
// the past hours. It returns the hour before and the hour now.
func (t *hourTable[K]) tick() (from, to int64) {
	t.ticks.Add(1)
	return t.advance()
}

// advance moves the clock to the current hour, sealing and expiring the past hours, without
// counting a tick.
func (t *hourTable[K]) advance() (from, to int64) {
	from = t.hour.Load()
	hour := unixHour(t.now())
	if hour <= from {
		return from, from
	}
	t.hour.Store(hour)
	t.seal(hour)
	t.expire(hour)
	return from, hour
}

func (t *hourTable[K]) acquire(key K) *hourEntry[K] {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.lookup(key)
	if !t.isOverflow(e) {
		e.refs++
	}
	return e
}

func (t *hourTable[K]) isOverflow(e *hourEntry[K]) bool {
	return slices.Contains(t.overflow, e)
}

// entryOf returns the registered entry of key without taking a reference.
func (t *hourTable[K]) entryOf(key K) *hourEntry[K] {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lookup(key)
}

func (t *hourTable[K]) lookup(key K) *hourEntry[K] {
	if e, ok := t.entries[key]; ok {
		return e
	}
	if len(t.entries) >= t.maxEntries {
		return t.overflow[t.overflowIndex(key)]
	}
	e := &hourEntry[K]{table: t, key: key}
	t.entries[key] = e
	return e
}

func (t *hourTable[K]) release(e *hourEntry[K]) {
	if t.isOverflow(e) {
		return
	}
	t.mu.Lock()
	e.refs--
	t.mu.Unlock()
}

func (t *hourTable[K]) allEntries() []*hourEntry[K] {
	t.mu.Lock()
	defer t.mu.Unlock()
	entries := make([]*hourEntry[K], 0, len(t.entries)+len(t.overflow))
	for _, e := range t.entries {
		entries = append(entries, e)
	}
	return append(entries, t.overflow...)
}

// seal marks the buckets of hours before hour as sealed, waits for the counts already in
// them to finish, and then publishes hour-1 as settled. The clock has moved to hour before
// this runs, so new buckets are only made for hour or later.
func (t *hourTable[K]) seal(hour int64) {
	var pending []*hourBucket
	for _, e := range t.allEntries() {
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
	t.settled.Store(hour - 1)
}

// drain waits for the counts already in a bucket to finish. Once admit has started to refuse,
// a count not waited for here finds it refusing and leaves nothing behind.
func (t *hourTable[K]) drain() {
	var buckets []*hourBucket
	for _, e := range t.allEntries() {
		e.mu.Lock()
		buckets = append(buckets, e.buckets...)
		e.mu.Unlock()
	}
	for _, b := range buckets {
		for b.inflight.Load() != 0 {
			runtime.Gosched()
		}
	}
}

// expire drops the hours no longer kept and removes the entries left idle.
func (t *hourTable[K]) expire(hour int64) {
	t.snapshotMu.Lock()
	defer t.snapshotMu.Unlock()
	retainedSince := t.retainedSince.Load()
	if hour-t.retainHours > retainedSince {
		retainedSince = hour - t.retainHours
		t.retainedSince.Store(retainedSince)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	for key, e := range t.entries {
		if e.dropBefore(retainedSince, hour, true) {
			delete(t.entries, key)
		}
	}
	for _, e := range t.overflow {
		e.dropBefore(retainedSince, hour, false)
	}
}
