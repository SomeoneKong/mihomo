package statistic

import (
	"cmp"
	"slices"
	"sort"
	"sync"
	"time"
)

const (
	// ClosedRetain is how long the closed connections are kept: a little longer than the
	// 30 minutes clients look back, so a connection at the edge has not just expired.
	ClosedRetain = 31 * time.Minute
	// ClosedMaxRecords bounds the number of closed connections kept.
	ClosedMaxRecords = 10000
	// ClosedDefaultLimit is the number of records a read returns when it does not say.
	ClosedDefaultLimit = 1000
)

// DefaultClosed keeps the exit connections closed recently.
var DefaultClosed *ClosedConns

type closedRecord struct {
	seq      int64
	info     *TrackerInfo
	closedAt time.Time
}

// ClosedConns keeps the recently closed exit connections in the order of their sequence
// numbers. A record shows the connection's own totals as they are when read; traffic
// counted after the close moves it to the end with a new sequence number, so a reader
// that follows the numbers reads it again.
type ClosedConns struct {
	now        func() time.Time
	instance   string
	startedAt  time.Time
	retain     time.Duration
	maxRecords int

	mu           sync.Mutex
	records      []*closedRecord // ascending by seq from head
	head         int
	seq          int64     // the latest sequence number given
	evictedSeq   int64     // the highest sequence number dropped
	evictedUntil time.Time // the latest close time among the records dropped for room
	// oldest is no later than the earliest close time kept: a renewed record keeps its close
	// time at the end of the order, so expiry cannot stop at the first record kept.
	oldest time.Time
}

func newClosedConns(now func() time.Time, instance string, startedAt time.Time) *ClosedConns {
	return &ClosedConns{
		now:        now,
		instance:   instance,
		startedAt:  startedAt,
		retain:     ClosedRetain,
		maxRecords: ClosedMaxRecords,
	}
}

func (c *ClosedConns) add(counter *connCounter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	record := &closedRecord{seq: c.seq, info: counter.info, closedAt: c.now()}
	counter.record = record
	c.records = append(c.records, record)
	if c.oldest.IsZero() || record.closedAt.Before(c.oldest) {
		c.oldest = record.closedAt
	}
	c.evictLocked()
}

// renew moves the record of a connection counted after its close to the end.
func (c *ClosedConns) renew(counter *connCounter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := counter.record
	if record == nil {
		// Not published yet: its first read will show the totals with this count.
		return
	}
	live := c.records[c.head:]
	i, found := slices.BinarySearchFunc(live, record.seq, func(r *closedRecord, seq int64) int {
		return cmp.Compare(r.seq, seq)
	})
	if !found || live[i] != record {
		return // dropped already
	}
	c.records = slices.Delete(c.records, c.head+i, c.head+i+1)
	c.seq++
	record.seq = c.seq
	c.records = append(c.records, record)
}

// evictLocked drops the expired records wherever they are, then the lowest numbered ones
// beyond the room kept.
func (c *ClosedConns) evictLocked() {
	if now := c.now(); !c.oldest.IsZero() && now.Sub(c.oldest) > c.retain {
		kept := make([]*closedRecord, 0, len(c.records)-c.head)
		c.oldest = time.Time{}
		for _, record := range c.records[c.head:] {
			if now.Sub(record.closedAt) > c.retain {
				c.evictedSeq = maxInt64(c.evictedSeq, record.seq)
				continue
			}
			kept = append(kept, record)
			if c.oldest.IsZero() || record.closedAt.Before(c.oldest) {
				c.oldest = record.closedAt
			}
		}
		c.records, c.head = kept, 0
	}
	for len(c.records)-c.head > c.maxRecords {
		record := c.records[c.head]
		if record.closedAt.After(c.evictedUntil) {
			c.evictedUntil = record.closedAt
		}
		c.evictedSeq = maxInt64(c.evictedSeq, record.seq)
		c.records[c.head] = nil
		c.head++
	}
	if c.head > 1024 && c.head*2 > len(c.records) {
		c.records = slices.Clone(c.records[c.head:])
		c.head = 0
	}
}

// ClosedConnection is a closed connection as the connections list shows it, with its
// sequence number and the time it was closed.
type ClosedConnection struct {
	*TrackerInfo
	Seq      int64     `json:"seq"`
	ClosedAt time.Time `json:"closedAt"`
}

// ClosedSnapshot is a page of the closed connections.
//
// Oldest and Latest are the lowest sequence number kept and the latest given. Every
// connection closed after EvictedUntil, within ClosedRetain, is kept; it is null when none
// was dropped for room. Truncated is set when records after the one read last were dropped.
type ClosedSnapshot struct {
	Instance     string             `json:"instance"`
	StartedAt    time.Time          `json:"startedAt"`
	Oldest       int64              `json:"oldest"`
	Latest       int64              `json:"latest"`
	EvictedUntil *time.Time         `json:"evictedUntil"`
	Truncated    bool               `json:"truncated,omitempty"`
	Records      []ClosedConnection `json:"records"`
}

// Snapshot returns up to limit records with sequence numbers after after, ascending.
func (c *ClosedConns) Snapshot(after int64, limit int) *ClosedSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictLocked()
	snapshot := &ClosedSnapshot{
		Instance:  c.instance,
		StartedAt: c.startedAt,
		Latest:    c.seq,
		Truncated: after < c.evictedSeq,
		Records:   []ClosedConnection{},
	}
	if !c.evictedUntil.IsZero() {
		evictedUntil := c.evictedUntil
		snapshot.EvictedUntil = &evictedUntil
	}
	live := c.records[c.head:]
	if len(live) > 0 {
		snapshot.Oldest = live[0].seq
	}
	i := sort.Search(len(live), func(i int) bool { return live[i].seq > after })
	for _, record := range live[i:] {
		if len(snapshot.Records) >= limit {
			break
		}
		snapshot.Records = append(snapshot.Records, ClosedConnection{
			TrackerInfo: record.info,
			Seq:         record.seq,
			ClosedAt:    record.closedAt,
		})
	}
	return snapshot
}
