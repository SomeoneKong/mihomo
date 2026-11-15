package probe

import (
	"encoding/json"
	"sync"
	"time"
)

// Context kinds.
const (
	ContextParams     = "params"
	ContextConfig     = "config"
	ContextExclusions = "exclusions"
)

const timeLayout = "2006-01-02T15:04:05.000Z07:00"

// formatTime formats a time as the records do: RFC 3339 in UTC with milliseconds.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(timeLayout)
}

type storedEvent struct {
	seq  uint64
	data json.RawMessage
}

type contextKey struct {
	kind    string
	version uint64
}

type contextEntry struct {
	data json.RawMessage
	// lastSeq is the latest event that referenced the context; a context no event in the ring
	// refers to any more is dropped unless it is current.
	lastSeq uint64
	// replaced is when a later version became current. A context stays a while after, for
	// those that took its version just before and are yet to refer to it.
	replaced time.Time
}

// contextGrace is how long a replaced context stays without any reference.
const contextGrace = time.Minute

// signalMinute caps the signals of one group recorded one by one within a minute.
type signalMinute struct {
	minute  int64
	count   int
	dropped map[string]int
}

// Records is the ring of decision records of this process, with the contexts that explain
// them.
type Records struct {
	mutex     sync.Mutex
	instance  string
	startedAt time.Time
	now       func() time.Time

	ring []storedEvent
	head int // index of the oldest event
	size int
	next uint64

	config     uint64 // current versions
	exclusions uint64
	contexts   map[contextKey]*contextEntry
	// pins counts the needs and executions still to be recorded that refer to a context.
	pins map[contextKey]int

	signals    map[string]*signalMinute
	flushTimer *time.Timer
}

func newRecords(instance string, startedAt time.Time, now func() time.Time, capacity int) *Records {
	return &Records{
		instance:  instance,
		startedAt: startedAt,
		now:       now,
		ring:      make([]storedEvent, capacity),
		next:      1,
		contexts:  map[contextKey]*contextEntry{},
		pins:      map[contextKey]int{},
		signals:   map[string]*signalMinute{},
	}
}

// Append records an event of kind with fields and returns its sequence number. fields may be
// nil; the common fields are added to it.
func (r *Records) Append(kind string, fields map[string]any) uint64 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.appendLocked(kind, fields)
}

func (r *Records) appendLocked(kind string, fields map[string]any, refs ...contextKey) uint64 {
	if fields == nil {
		fields = map[string]any{}
	}
	seq := r.next
	r.next++
	fields["seq"] = seq
	fields["time"] = formatTime(r.now())
	fields["kind"] = kind
	fields["config"] = r.config
	fields["exclusions"] = r.exclusions
	data, err := json.Marshal(fields)
	if err != nil {
		data, _ = json.Marshal(map[string]any{"seq": seq, "time": fields["time"], "kind": kind, "error": err.Error()})
	}
	if r.size == len(r.ring) {
		r.head = (r.head + 1) % len(r.ring)
		r.size--
	}
	r.ring[(r.head+r.size)%len(r.ring)] = storedEvent{seq: seq, data: data}
	r.size++
	r.touchContext(ContextConfig, r.config, seq)
	r.touchContext(ContextExclusions, r.exclusions, seq)
	for _, ref := range refs {
		r.touchContext(ref.kind, ref.version, seq)
	}
	r.dropUnreferencedContexts()
	return seq
}

// appendPerformance records a performance event: the memory of the failures, the attribution
// of failures to common events, the checks of the local network and the outages of pools.
func (r *Records) appendPerformance(fields map[string]any) uint64 {
	return r.Append("performance", fields)
}

// AppendAt records an event like Append, which also refers to the contexts of the given
// versions, as a need taken under an earlier config does; they are kept while it is.
func (r *Records) AppendAt(kind string, fields map[string]any, config, exclusions uint64) uint64 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.appendLocked(kind, fields, contextKey{ContextConfig, config}, contextKey{ContextExclusions, exclusions})
}

// PinCurrent pins the current versions and returns them, at once, so that they cannot be
// dropped between being read and being pinned.
func (r *Records) PinCurrent() (config, exclusions uint64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.pins[contextKey{ContextConfig, r.config}]++
	r.pins[contextKey{ContextExclusions, r.exclusions}]++
	return r.config, r.exclusions
}

// Pin keeps the contexts of the given versions until Unpin, for a need or an execution that
// is still to be recorded.
func (r *Records) Pin(config, exclusions uint64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.pins[contextKey{ContextConfig, config}]++
	r.pins[contextKey{ContextExclusions, exclusions}]++
}

// Unpin releases what Pin kept.
func (r *Records) Unpin(config, exclusions uint64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for _, key := range []contextKey{{ContextConfig, config}, {ContextExclusions, exclusions}} {
		if r.pins[key] <= 1 {
			delete(r.pins, key)
		} else {
			r.pins[key]--
		}
	}
	r.dropUnreferencedContexts()
}

func (r *Records) touchContext(kind string, version, seq uint64) {
	if entry, ok := r.contexts[contextKey{kind, version}]; ok && entry.lastSeq < seq {
		entry.lastSeq = seq
	}
}

func (r *Records) oldestLocked() uint64 {
	if r.size == 0 {
		return r.next
	}
	return r.ring[r.head].seq
}

func (r *Records) dropUnreferencedContexts() {
	oldest := r.oldestLocked()
	now := r.now()
	for key, entry := range r.contexts {
		if key.kind == ContextParams {
			continue
		}
		current := (key.kind == ContextConfig && key.version == r.config) ||
			(key.kind == ContextExclusions && key.version == r.exclusions)
		if current || r.pins[key] > 0 || entry.lastSeq >= oldest || now.Sub(entry.replaced) < contextGrace {
			continue
		}
		delete(r.contexts, key)
	}
}

// Versions returns the current config and exclusions versions.
func (r *Records) Versions() (config, exclusions uint64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.config, r.exclusions
}

func (r *Records) setParams(data any) {
	encoded, _ := json.Marshal(data)
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.contexts[contextKey{ContextParams, 1}] = &contextEntry{data: encoded}
}

// SetConfig stores a new config context and makes it current; it returns its version.
func (r *Records) SetConfig(data map[string]any) uint64 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.replaceLocked(ContextConfig, r.config)
	r.config++
	r.storeContextLocked(ContextConfig, r.config, data)
	return r.config
}

func (r *Records) replaceLocked(kind string, version uint64) {
	if entry, ok := r.contexts[contextKey{kind, version}]; ok {
		entry.replaced = r.now()
	}
}

// SetExclusions stores the exclusions context of a version of the table and makes it current,
// recording the change as an event when event is set. A version older than the current one is
// ignored.
func (r *Records) SetExclusions(version uint64, data map[string]any, event bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if version <= r.exclusions {
		return
	}
	r.replaceLocked(ContextExclusions, r.exclusions)
	r.exclusions = version
	r.storeContextLocked(ContextExclusions, version, data)
	if event {
		r.appendLocked("exclusions", map[string]any{"pools": data["pools"]})
	}
}

func (r *Records) storeContextLocked(kind string, version uint64, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}
	data["kind"] = kind
	data["version"] = version
	data["time"] = formatTime(r.now())
	encoded, _ := json.Marshal(data)
	// kept while current; once replaced, only while an event refers to it or it is pinned
	r.contexts[contextKey{kind, version}] = &contextEntry{data: encoded}
	r.dropUnreferencedContexts()
}

// EventPage is a page of records.
type EventPage struct {
	Schema    int               `json:"schema"`
	Instance  string            `json:"instance"`
	StartedAt time.Time         `json:"startedAt"`
	Oldest    uint64            `json:"oldest"`
	Latest    uint64            `json:"latest"`
	Events    []json.RawMessage `json:"events"`
}

// Events returns up to limit events after the sequence number after.
func (r *Records) Events(after uint64, limit int) EventPage {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	page := EventPage{
		Schema:    SchemaVersion,
		Instance:  r.instance,
		StartedAt: r.startedAt,
		Oldest:    r.oldestLocked(),
		Latest:    r.next - 1,
		Events:    []json.RawMessage{},
	}
	if r.size == 0 || after >= page.Latest {
		return page
	}
	start := 0
	if after >= page.Oldest {
		start = int(after - page.Oldest + 1)
	}
	for i := start; i < r.size && len(page.Events) < limit; i++ {
		page.Events = append(page.Events, r.ring[(r.head+i)%len(r.ring)].data)
	}
	return page
}

// ContextPage holds the contexts asked for.
type ContextPage struct {
	Schema      int               `json:"schema"`
	Instance    string            `json:"instance"`
	Contexts    []json.RawMessage `json:"contexts"`
	Unavailable []ContextRef      `json:"unavailable"`
}

// ContextRef names a context.
type ContextRef struct {
	Kind    string `json:"kind"`
	Version uint64 `json:"version"`
}

// Contexts returns the params context and the asked config and exclusions versions; with no
// versions asked, every context still kept.
func (r *Records) Contexts(configs, exclusions []uint64) ContextPage {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	page := ContextPage{Schema: SchemaVersion, Instance: r.instance, Contexts: []json.RawMessage{}, Unavailable: []ContextRef{}}
	if entry, ok := r.contexts[contextKey{ContextParams, 1}]; ok {
		page.Contexts = append(page.Contexts, entry.data)
	}
	if len(configs) == 0 && len(exclusions) == 0 {
		for key, entry := range r.contexts {
			if key.kind != ContextParams {
				page.Contexts = append(page.Contexts, entry.data)
			}
		}
		return page
	}
	add := func(kind string, versions []uint64) {
		for _, version := range versions {
			if entry, ok := r.contexts[contextKey{kind, version}]; ok {
				page.Contexts = append(page.Contexts, entry.data)
			} else {
				page.Unavailable = append(page.Unavailable, ContextRef{Kind: kind, Version: version})
			}
		}
	}
	add(ContextConfig, configs)
	add(ContextExclusions, exclusions)
	return page
}

// Signal records a failure signal of group. Signals that form an obligation or start a
// verification are always recorded; others are capped per group and minute, and the ones
// left out are counted in one record at the end of the minute.
func (r *Records) Signal(group, disposition string, fields map[string]any) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	minute := r.now().Unix() / 60
	window := r.signals[group]
	if window == nil || window.minute != minute {
		r.flushSignalLocked(group, window)
		window = &signalMinute{minute: minute, dropped: map[string]int{}}
		r.signals[group] = window
	}
	always := disposition == "obligation" || disposition == "verify"
	if !always {
		if window.count >= SignalsPerMinute {
			window.dropped[disposition]++
			r.scheduleFlushLocked(minute)
			return
		}
		window.count++
	}
	if fields == nil {
		fields = map[string]any{}
	}
	fields["group"] = group
	fields["disposition"] = disposition
	r.appendLocked("signal", fields)
}

// Limited records an event of a group that shares the cap of its signals: one that always
// counts is recorded regardless, the others left out are counted with the signals, by event.
func (r *Records) Limited(group string, always bool, kind, event string, fields map[string]any) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	minute := r.now().Unix() / 60
	window := r.signals[group]
	if window == nil || window.minute != minute {
		r.flushSignalLocked(group, window)
		window = &signalMinute{minute: minute, dropped: map[string]int{}}
		r.signals[group] = window
	}
	if !always {
		if window.count >= SignalsPerMinute {
			window.dropped[event]++
			r.scheduleFlushLocked(minute)
			return
		}
		window.count++
	}
	if fields == nil {
		fields = map[string]any{}
	}
	fields["group"] = group
	fields["event"] = event
	r.appendLocked(kind, fields)
}

// FlushSignals records the counts of signals left out in minutes that have ended.
func (r *Records) FlushSignals() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.flushTimer != nil {
		r.flushTimer.Stop()
		r.flushTimer = nil
	}
	minute := r.now().Unix() / 60
	for group, window := range r.signals {
		if window.minute != minute {
			r.flushSignalLocked(group, window)
			delete(r.signals, group)
		} else if len(window.dropped) > 0 {
			r.scheduleFlushLocked(minute)
		}
	}
}

// scheduleFlushLocked counts what was left out in a minute once it ends, even if no signal
// follows.
func (r *Records) scheduleFlushLocked(minute int64) {
	if r.flushTimer == nil {
		r.flushTimer = time.AfterFunc(time.Unix((minute+1)*60, 0).Sub(r.now())+time.Second, r.FlushSignals)
	}
}

func (r *Records) flushSignalLocked(group string, window *signalMinute) {
	if window == nil || len(window.dropped) == 0 {
		return
	}
	total := 0
	for _, count := range window.dropped {
		total += count
	}
	r.appendLocked("signal", map[string]any{
		"group":       group,
		"disposition": "dropped",
		"count":       total,
		"dropped":     window.dropped,
		"minute":      formatTime(time.Unix(window.minute*60, 0)),
	})
	window.dropped = map[string]int{}
}
