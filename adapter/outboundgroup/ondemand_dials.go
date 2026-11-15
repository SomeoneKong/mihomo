package outboundgroup

import (
	"sort"
	"sync/atomic"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// How a real TCP dial of a group ended for its caller. A dial ends once, by what came first;
// delivered only tells that a member handed out a connection, whose end is still to come.
const (
	dialDelivered        = "delivered"
	dialNoMember         = "noMember"         // the group had no member to dial
	dialRejected         = "rejected"         // the group chose a REJECT member
	dialFailed           = "dialFailed"       // the members dialed all failed
	dialCanceled         = "canceled"         // the caller canceled the dial
	dialUnobserved       = "unobserved"       // delivered by a member the group does not watch
	dialFirstData        = "firstData"        // the connection received its first data
	dialFailedBeforeData = "failedBeforeData" // the first write or read failed before any data
	dialClosedSent       = "closedSent"       // closed without data after something was sent
	dialClosedUnsent     = "closedUnsent"     // closed without data, nothing sent
)

// groupDial is one real TCP dial of a group, from the time the group took it; the connection
// that carries it reports its end. The group counts it with the minute it started in and the
// config version of then.
type groupDial struct {
	od         *onDemand
	start      time.Time
	config     uint64
	exclusions uint64
	ended      atomic.Bool
	// preferred is the preference in effect when the dial started, nil for none
	preferred C.Proxy
}

func (od *onDemand) newGroupDial() *groupDial {
	config, exclusions := od.coord.Records().Versions()
	d := &groupDial{od: od, start: od.now(), config: config, exclusions: exclusions}
	// the pin as set now decides, as it does for the route
	if pin := od.pin.Load(); pin == nil || *pin == "" {
		d.preferred = od.snap.Load().preferred
	}
	return d
}

// dialCountNote tells a group of a dial it took: returned when the dial returned, which
// counts it, with outcome how it ended there or delivered; otherwise how a delivered one
// ended.
type dialCountNote struct {
	start      time.Time
	config     uint64
	exclusions uint64
	returned   bool
	outcome    string
	launched   bool // a candidate was dialed beside or after the route
	preferred  bool // delivered by the preference in effect when it started
	wait       time.Duration
}

// returned counts the dial once it returned; outcome is how it ended there, or delivered.
func (d *groupDial) returned(outcome string, launched bool) {
	d.returnedFrom(nil, outcome, launched)
}

func (d *groupDial) returnedFrom(member C.Proxy, outcome string, launched bool) {
	if d == nil || outcome != dialDelivered && !d.ended.CompareAndSwap(false, true) {
		return
	}
	preferred := member != nil && member == d.preferred
	d.od.post(dialCountNote{start: d.start, config: d.config, exclusions: d.exclusions, returned: true, outcome: outcome, launched: launched, preferred: preferred})
}

// end records how a delivered dial ended, the first end only.
func (d *groupDial) end(outcome string) {
	if d == nil || !d.ended.CompareAndSwap(false, true) {
		return
	}
	note := dialCountNote{start: d.start, config: d.config, exclusions: d.exclusions, outcome: outcome}
	if outcome == dialFirstData {
		note.wait = d.od.now().Sub(d.start)
	}
	d.od.post(note)
}

// returnedBy counts a dial a member handed out a connection for, which the group watches or
// not: one not watched ends there, as rejected for a REJECT member.
func (d *groupDial) returnedBy(member C.Proxy, watched bool, launched bool) {
	switch {
	case watched:
		d.returnedFrom(member, dialDelivered, launched)
	case member.Type() == C.Reject || member.Type() == C.RejectDrop:
		d.returned(dialRejected, launched)
	default:
		d.returnedFrom(member, dialUnobserved, launched)
	}
}

// dialKey is the minute dials started in and the config version of then.
type dialKey struct {
	minute int64
	config uint64
}

// dialCounts counts the dials of a minute by how they ended; the ones without an end when
// they are summed up are unknown.
type dialCounts struct {
	exclusions uint64
	dials      int
	launched   int
	preferred  int    // delivered by the preference in effect when they started
	waits      [3]int // first data within 1 second, within 3 seconds, later
	outcomes   map[string]int
}

func (od *onDemand) onDialCount(n dialCountNote) {
	minute := n.start.Unix() / 60
	if minute <= od.summarized {
		return // already summed up
	}
	key := dialKey{minute: minute, config: n.config}
	c := od.dialCounts[key]
	if c == nil {
		c = &dialCounts{exclusions: n.exclusions, outcomes: map[string]int{}}
		od.dialCounts[key] = c
	}
	if n.returned {
		c.dials++
		if n.launched {
			c.launched++
		}
		if n.preferred {
			c.preferred++
		}
		if n.outcome == dialDelivered {
			return
		}
	}
	if n.outcome == dialFirstData {
		switch {
		case n.wait <= time.Second:
			c.waits[0]++
		case n.wait <= 3*time.Second:
			c.waits[1]++
		default:
			c.waits[2]++
		}
		return
	}
	c.outcomes[n.outcome]++
}

// flushDialCounts records the counts of the minutes up to last, in order, and forgets them.
func (od *onDemand) flushDialCounts(last int64) {
	var keys []dialKey
	for key := range od.dialCounts {
		if key.minute <= last {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].minute != keys[j].minute {
			return keys[i].minute < keys[j].minute
		}
		return keys[i].config < keys[j].config
	})
	for _, key := range keys {
		c := od.dialCounts[key]
		delete(od.dialCounts, key)
		ended := c.waits[0] + c.waits[1] + c.waits[2]
		for _, count := range c.outcomes {
			ended += count
		}
		fields := od.event("dials")
		fields["minute"] = formatTime(time.Unix(key.minute*60, 0))
		fields["dialConfig"] = key.config
		fields["dials"] = c.dials
		fields["launched"] = c.launched
		fields["preferred"] = c.preferred
		fields["firstData"] = map[string]int{"within1s": c.waits[0], "within3s": c.waits[1], "later": c.waits[2]}
		for _, outcome := range []string{dialNoMember, dialRejected, dialFailed, dialCanceled, dialUnobserved, dialFailedBeforeData, dialClosedSent, dialClosedUnsent} {
			fields[outcome] = c.outcomes[outcome]
		}
		unknown := c.dials - ended
		if unknown < 0 {
			unknown = 0
		}
		fields["unknown"] = unknown
		fields["transport"] = "tcp"
		od.coord.Records().AppendAt("group", fields, key.config, c.exclusions)
	}
}

// inheritDialCounts adds the counts of the group replaced to those of this one.
func (od *onDemand) inheritDialCounts(counts map[dialKey]*dialCounts) {
	for key, c := range counts {
		if key.minute <= od.summarized {
			continue
		}
		own := od.dialCounts[key]
		if own == nil {
			od.dialCounts[key] = c
			continue
		}
		own.dials += c.dials
		own.launched += c.launched
		own.preferred += c.preferred
		for i := range own.waits {
			own.waits[i] += c.waits[i]
		}
		for outcome, count := range c.outcomes {
			own.outcomes[outcome] += count
		}
	}
}
