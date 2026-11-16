package statistic

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// ConnStatsRetainHours is how many hours before the current one the connection stats keep.
	ConnStatsRetainHours = 72
	// ConnStatsMaxEntries bounds the number of keys counted individually.
	ConnStatsMaxEntries = 20000
	// ConnStatsOverflowName fills every field of the entry that counts the keys beyond
	// ConnStatsMaxEntries.
	ConnStatsOverflowName = "*overflow*"
)

// DefaultConnStats counts the connections of every tracked exit connection per key and hour,
// while enabled.
var DefaultConnStats *ConnStats

// ConnStatsKey is what a connection is counted under: where it came from, the process that
// made it, its target, network, rule and route.
type ConnStatsKey struct {
	InType      string
	InName      string
	InUser      string
	SrcIP       string
	Process     string // the path of the process, or its name when the path is unknown
	Target      string // the host, else the sniffed host, else the destination IP
	TargetIsIP  bool
	Network     string
	Rule        string
	RulePayload string
	Provider    string // of the proxy that made the connection
	Chain       string // the chain, JSON encoded
}

func connStatsKeyOf(info *TrackerInfo) ConnStatsKey {
	key := ConnStatsKey{Rule: info.Rule, RulePayload: info.RulePayload}
	if m := info.Metadata; m != nil {
		key.InType = m.Type.String()
		key.InName = m.InName
		key.InUser = m.InUser
		key.Network = m.NetWork.String()
		if m.SrcIP.IsValid() {
			key.SrcIP = m.SrcIP.Unmap().String()
		}
		key.Process = m.ProcessPath
		if key.Process == "" {
			key.Process = m.Process
		}
		switch {
		case m.Host != "":
			key.Target = m.Host
		case m.SniffHost != "":
			key.Target = m.SniffHost
		case m.DstIP.IsValid():
			key.Target = m.DstIP.Unmap().String()
			key.TargetIsIP = true
		}
	}
	if len(info.ProviderChain) > 0 {
		key.Provider = info.ProviderChain[0]
	}
	chain := []string(info.Chain)
	if chain == nil {
		chain = []string{}
	}
	encoded, _ := json.Marshal(chain)
	key.Chain = string(encoded)
	return key
}

const (
	countingOn uint8 = 1 << iota
	countingOff
)

// ConnStats counts connections and their traffic per ConnStatsKey in UTC hours, while
// enabled. Disabling it stops the counting; what was counted stays readable until it expires.
//
// countingMu is taken before snapshotMu: the clock, the switch and snapshots hold it so that
// an hour's counting state and its buckets tell the same story.
type ConnStats struct {
	*hourTable[ConnStatsKey]
	instance  string
	startedAt time.Time
	// enabled admits counts; reported, what readers are told, turns off only once the counts
	// admitted before are in.
	enabled  atomic.Bool
	reported atomic.Bool

	countingMu sync.Mutex
	configured bool            // enabled has been set by a config
	counting   map[int64]uint8 // per hour, whether it was seen counting and not counting
}

func newConnStats(now func() time.Time, instance string, startedAt time.Time) *ConnStats {
	overflow := ConnStatsKey{
		InType: ConnStatsOverflowName, InName: ConnStatsOverflowName, InUser: ConnStatsOverflowName,
		SrcIP: ConnStatsOverflowName, Process: ConnStatsOverflowName, Target: ConnStatsOverflowName,
		Network: ConnStatsOverflowName, Rule: ConnStatsOverflowName, RulePayload: ConnStatsOverflowName,
		Provider: ConnStatsOverflowName, Chain: `["` + ConnStatsOverflowName + `"]`,
	}
	table := newHourTable(now, ConnStatsRetainHours, ConnStatsMaxEntries, []ConnStatsKey{overflow},
		func(ConnStatsKey) int { return 0 })
	s := &ConnStats{
		hourTable: table,
		instance:  instance,
		startedAt: startedAt,
		counting:  make(map[int64]uint8),
	}
	table.admit = s.enabled.Load
	return s
}

func (s *ConnStats) run() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		s.tick()
	}
}

func (s *ConnStats) tick() {
	s.countingMu.Lock()
	defer s.countingMu.Unlock()
	s.passed(s.hourTable.tick())
}

// passed marks the hours the clock moved through as the switch had them, and forgets those
// no longer kept; the caller holds countingMu.
func (s *ConnStats) passed(from, to int64) {
	if to == from {
		return
	}
	if s.configured {
		bit := countingBit(s.enabled.Load())
		for hour := maxInt64(from+1, to-s.retainHours); hour <= to; hour++ {
			s.counting[hour] |= bit
		}
	}
	retainedSince := s.retainedSince.Load()
	for hour := range s.counting {
		if hour < retainedSince {
			delete(s.counting, hour)
		}
	}
}

func countingBit(enabled bool) uint8 {
	if enabled {
		return countingOn
	}
	return countingOff
}

// Enabled reports whether connections are counted; once it reports false, nothing more is.
func (s *ConnStats) Enabled() bool {
	return s.reported.Load()
}

// SetEnabled starts or stops the counting, returning once a stop is complete: the counts
// admitted before it are in. Connections already open are counted from their next traffic
// on, without being counted as new.
func (s *ConnStats) SetEnabled(enabled bool) {
	s.countingMu.Lock()
	defer s.countingMu.Unlock()
	// the hours passed since the last tick went by as the switch was
	s.passed(s.hourTable.advance())
	s.configured = true
	s.counting[s.hour.Load()] |= countingBit(enabled)
	s.enabled.Store(enabled)
	if !enabled {
		s.drain()
	}
	s.reported.Store(enabled)
}

// ConnStatsBucket is what one key counted in one hour: the traffic, and the connections
// opened in that hour.
type ConnStatsBucket struct {
	Hour  int64 `json:"hour"`
	Up    int64 `json:"up"`
	Down  int64 `json:"down"`
	Conns int64 `json:"conns"`
}

// ConnStatsEntry is what one key counted.
type ConnStatsEntry struct {
	InType      string            `json:"inType"`
	InName      string            `json:"inName"`
	InUser      string            `json:"inUser"`
	SrcIP       string            `json:"srcIP"`
	Process     string            `json:"process"`
	Target      string            `json:"target"`
	TargetIsIP  bool              `json:"targetIsIP"`
	Network     string            `json:"network"`
	Rule        string            `json:"rule"`
	RulePayload string            `json:"rulePayload"`
	Provider    string            `json:"provider"`
	Chain       []string          `json:"chain"`
	Buckets     []ConnStatsBucket `json:"buckets"`
}

// ConnStatsCounting lists, as inclusive ranges of hours, the hours connections were counted
// throughout and those they were counted for a part of. The other hours were not counted.
type ConnStatsCounting struct {
	On      [][2]int64 `json:"on"`
	Partial [][2]int64 `json:"partial"`
}

// ConnStatsSnapshot is the connection stats of a range of hours.
//
// Hours from RetainedSince to Settled are final. Later hours may still grow. Tick is the
// clock of the stats when the snapshot began: an incremental snapshot from tick-1 on has
// every entry counted after this one read it.
type ConnStatsSnapshot struct {
	Instance      string            `json:"instance"`
	Enabled       bool              `json:"enabled"`
	Counting      ConnStatsCounting `json:"counting"`
	Tick          int64             `json:"tick"`
	StartedAt     time.Time         `json:"startedAt"`
	Hour          int64             `json:"hour"`
	Settled       int64             `json:"settled"`
	RetainedSince int64             `json:"retainedSince"`
	Overflow      bool              `json:"overflow"`
	Entries       []ConnStatsEntry  `json:"entries"`
}

// Snapshot returns the non-empty buckets of hours from since to until, both inclusive. With
// incremental, only the entries counted at touchedSince or later are included.
func (s *ConnStats) Snapshot(since, until int64, incremental bool, touchedSince int64) *ConnStatsSnapshot {
	tick := s.ticks.Load()
	s.countingMu.Lock()
	s.snapshotMu.RLock()
	snapshot := &ConnStatsSnapshot{
		Instance:      s.instance,
		Enabled:       s.reported.Load(),
		Tick:          tick,
		StartedAt:     s.startedAt,
		RetainedSince: s.retainedSince.Load(),
		Settled:       s.settled.Load(),
		Hour:          s.hour.Load(),
		Entries:       []ConnStatsEntry{},
	}
	snapshot.Counting = s.countingOf(since, until, snapshot.RetainedSince, snapshot.Hour)
	s.countingMu.Unlock()
	for _, e := range s.allEntries() {
		if incremental && e.touched.Load() < touchedSince {
			continue
		}
		counts := e.collect(since, until)
		if len(counts) == 0 {
			continue
		}
		if s.isOverflow(e) {
			snapshot.Overflow = true
		}
		snapshot.Entries = append(snapshot.Entries, connStatsEntryOf(e.key, counts))
	}
	s.snapshotMu.RUnlock()
	return snapshot
}

func connStatsEntryOf(key ConnStatsKey, counts []hourCount) ConnStatsEntry {
	entry := ConnStatsEntry{
		InType: key.InType, InName: key.InName, InUser: key.InUser, SrcIP: key.SrcIP,
		Process: key.Process, Target: key.Target, TargetIsIP: key.TargetIsIP, Network: key.Network,
		Rule: key.Rule, RulePayload: key.RulePayload, Provider: key.Provider,
		Buckets: make([]ConnStatsBucket, 0, len(counts)),
	}
	_ = json.Unmarshal([]byte(key.Chain), &entry.Chain)
	if entry.Chain == nil {
		entry.Chain = []string{}
	}
	for _, c := range counts {
		entry.Buckets = append(entry.Buckets, ConnStatsBucket{Hour: c.hour, Up: c.up, Down: c.down, Conns: c.conns})
	}
	return entry
}

// countingOf lists the counting state of the hours asked for; the caller holds countingMu.
func (s *ConnStats) countingOf(since, until, retainedSince, hour int64) ConnStatsCounting {
	counting := ConnStatsCounting{On: [][2]int64{}, Partial: [][2]int64{}}
	from := maxInt64(since, maxInt64(retainedSince, unixHour(s.startedAt)))
	to := minInt64(until, hour)
	appendHour := func(ranges [][2]int64, hour int64) [][2]int64 {
		if n := len(ranges); n > 0 && ranges[n-1][1] == hour-1 {
			ranges[n-1][1] = hour
			return ranges
		}
		return append(ranges, [2]int64{hour, hour})
	}
	for h := from; h <= to; h++ {
		switch s.counting[h] {
		case countingOn:
			counting.On = appendHour(counting.On, h)
		case countingOn | countingOff:
			counting.Partial = appendHour(counting.Partial, h)
		}
	}
	return counting
}

// connCounter counts an exit connection in the connection stats, and records it among the
// closed connections once it is closed.
type connCounter struct {
	stats  *ConnStats
	closed *ClosedConns
	info   *TrackerInfo

	entry    atomic.Pointer[hourEntry[ConnStatsKey]]
	isClosed atomic.Bool

	mu     sync.Mutex
	left   bool          // the tracker no longer holds a reference to the entry
	record *closedRecord // guarded by closed.mu
}

// newConnCounter counts the connection as opened in this hour if connections are counted;
// otherwise its traffic is counted once they are, and it is not counted as opened.
func newConnCounter(stats *ConnStats, closed *ClosedConns, info *TrackerInfo) *connCounter {
	c := &connCounter{stats: stats, closed: closed, info: info}
	if stats.enabled.Load() {
		e := stats.acquire(connStatsKeyOf(info))
		c.entry.Store(e)
		e.add(0, 0, 1)
	}
	return c
}

// add counts traffic the tracker has already added to its own totals, which a closed
// record shows, so that a count after the close has the record read again.
func (c *connCounter) add(up, down int64) {
	if up == 0 && down == 0 {
		return
	}
	if c.stats.enabled.Load() {
		e := c.entry.Load()
		if e == nil {
			e = c.attach()
		}
		e.add(up, down, 0)
	}
	if c.isClosed.Load() {
		c.closed.renew(c)
	}
}

func (c *connCounter) attach() *hourEntry[ConnStatsKey] {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entry.Load(); e != nil {
		return e
	}
	key := connStatsKeyOf(c.info)
	if c.left {
		// Traffic that outlived the tracker: count it without holding the entry.
		return c.stats.entryOf(key)
	}
	e := c.stats.acquire(key)
	c.entry.Store(e)
	return e
}

// leave releases the entry and records the connection as closed. It is marked closed
// before its record is published, so that a count either is in the totals the record
// shows when first read, or has the record read again.
func (c *connCounter) leave() {
	c.mu.Lock()
	c.left = true
	if e := c.entry.Load(); e != nil {
		c.stats.release(e)
	}
	c.mu.Unlock()
	c.isClosed.Store(true)
	c.closed.add(c)
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
