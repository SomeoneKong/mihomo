package probe

import (
	"math/rand"
	"sync"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// census tests every member of the on-demand groups once an interval, spread at random over
// the interval, whether or not the groups are in use. A member tested within the interval
// for another reason is not tested again.
type census struct {
	coordinator *Coordinator
	mutex       sync.Mutex
	reg         *Registration
	interval    time.Duration
	due         map[Key]time.Time
	timer       *time.Timer
	version     uint64
}

type censusGroup struct{}

func (censusGroup) Name() string                       { return "" }
func (censusGroup) References(C.Proxy, Condition) bool { return false }
func (censusGroup) Deliver(Notice)                     {}

func newCensus(c *Coordinator) *census {
	return &census{coordinator: c, due: map[Key]time.Time{}}
}

// SetCensus sets the census interval and the members it covers; zero turns it off. Members
// still covered keep their due times.
func (c *Coordinator) SetCensus(interval time.Duration, keys []Key) {
	c.census.reset(interval, keys)
}

func (cs *census) reset(interval time.Duration, keys []Key) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	if interval < 0 {
		interval = 0
	}
	if interval > 0 && interval < MinCensusInterval {
		interval = MinCensusInterval
	}
	if interval != cs.interval {
		cs.due = map[Key]time.Time{}
	}
	cs.interval = interval
	if interval == 0 {
		cs.due = map[Key]time.Time{}
		cs.submitLocked()
		return
	}
	if cs.reg == nil {
		cs.reg = cs.coordinator.registerPersistent(censusGroup{})
	}
	now := cs.coordinator.now()
	keep := make(map[Key]time.Time, len(keys))
	for _, key := range keys {
		// the groups list the direct members they test, and no other built-in one
		if IsBuiltin(key.Target) && key.Target.Type() != C.Direct {
			continue
		}
		if due, ok := cs.due[key]; ok {
			keep[key] = due
			continue
		}
		keep[key] = now.Add(time.Duration(rand.Int63n(int64(interval))))
	}
	cs.due = keep
	cs.tickLocked()
}

func (cs *census) tick() {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.tickLocked()
}

// tickLocked moves the members tested since they became due to their next due time, and
// asks for the members still due.
func (cs *census) tickLocked() {
	if cs.timer != nil {
		cs.timer.Stop()
		cs.timer = nil
	}
	if cs.interval == 0 {
		return
	}
	now := cs.coordinator.now()
	var next time.Time
	for key, due := range cs.due {
		if !due.After(now) {
			if ev := cs.coordinator.Evidence(key); !ev.Started.IsZero() && ev.Started.After(due.Add(-cs.interval)) {
				due = ev.Started.Add(cs.interval)
				cs.due[key] = due
			}
		}
		if due.After(now) && (next.IsZero() || due.Before(next)) {
			next = due
		}
	}
	cs.submitLocked()
	wake := now.Add(time.Minute)
	if !cs.hasDueLocked(now) && !next.IsZero() {
		wake = next
	}
	cs.timer = time.AfterFunc(wake.Sub(now), cs.tick)
}

func (cs *census) hasDueLocked(now time.Time) bool {
	for _, due := range cs.due {
		if !due.After(now) {
			return true
		}
	}
	return false
}

func (cs *census) submitLocked() {
	if cs.reg == nil {
		return
	}
	now := cs.coordinator.now()
	var needs []Need
	table := cs.coordinator.exclusions.Current()
	var evaluator *Evaluator
	if len(table.Pools) > 0 {
		evaluator = NewEvaluatorFor(table, cs.coordinator.Lookup)
	}
	config, _ := cs.coordinator.records.Versions()
	exclusions := table.Version
	for key, due := range cs.due {
		if due.After(now) {
			continue
		}
		if evaluator != nil {
			if excluded, _ := evaluator.Excluded(key.Target); excluded {
				continue
			}
		}
		// any test within the interval before it fell due does, as the tick counts it: the
		// interval excludes its start, the test a census was due an interval after
		needs = append(needs, Need{
			Target: key.Target, Cond: key.Cond, Class: ClassMaintain, Rank: rankCensus,
			Source: SourceCensus, NotBefore: due.Add(-cs.interval).Add(time.Nanosecond), Config: config, Exclusions: exclusions,
		})
	}
	cs.version++
	cs.reg.Submit(cs.version, needs)
}

// rankCensus puts the census after every other maintenance need.
const rankCensus = 1 << 20
