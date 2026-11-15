package probe

import (
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
	"github.com/stretchr/testify/require"
)

type memoryRecorder struct{ *recorder }

func (g memoryRecorder) Deliver(n Notice) {
	g.recorder.Deliver(n)
	if m, ok := n.(MemoryCleared); ok {
		m.Processed()
	}
}

func TestClearMemoryFailureBoundaryAndReconfirmation(t *testing.T) {
	for _, covered := range []bool{false, true} {
		for _, pending := range []bool{false, true} {
			name := map[bool]string{false: "Outside", true: "Inside"}[covered] + map[bool]string{false: "InFlight", true: "Pending"}[pending]
			t.Run(name, func(t *testing.T) {
				c, _, clock := newClockedCoordinator(t)
				a := pooledNode("A", "pool", "")
				key := Key{a, testCond}
				g := newRecorder("g", a)
				activeRegistration(c, memoryRecorder{g})
				old := startManual(c, a, testCond)
				clock.Advance(time.Second)
				if covered {
					c.network.past = []interval{{start: old.started, end: clock.Now()}}
				}
				failure := C.ProbeResult{ErrorClass: C.ProbeErrorDial}
				if pending {
					c.finish(old, failure, nil)
					require.True(t, lastCompletion(t, g, a).Charged)
				}
				require.Equal(t, MemoryClearResult{Clear: 1, Groups: 1}, c.ClearMemory())
				if pending {
					require.Equal(t, []Attributed{{Exec: old.id, Key: key, Started: old.started, Outcome: OutcomeUndecided, Reason: "cleared"}}, g.attributed())
				} else {
					c.finish(old, failure, nil)
					require.False(t, lastCompletion(t, g, a).Charged)
					require.Empty(t, g.attributed())
				}
				ev := c.Evidence(key)
				require.Equal(t, Dead, ev.State)
				require.Equal(t, old.started, ev.DeadSince)
				require.Equal(t, old.started, ev.charged)
				require.Equal(t, covered, ev.chargedLocal)
				require.False(t, ev.NextAllowed.IsZero())
				require.Empty(t, c.quality.pending)
				if !covered {
					require.Len(t, c.outages.recent[poolKey{"pool", testCond}], 1)
				}
				// Reconfirmation still works after the original coverage has expired.
				c.network.past = nil
				retry := startManual(c, a, testCond)
				require.Equal(t, c.MemoryBoundary().At, retry.started)
				c.finish(retry, failure, nil)
				require.Equal(t, covered, lastCompletion(t, g, a).Charged)
				clock.Advance(time.Second)
				c.finish(startManual(c, a, testCond), failure, nil)
				require.False(t, lastCompletion(t, g, a).Charged)
				clock.Advance(AttributionDelay)
				c.AttributeDue()
				want := Counts{}
				if covered {
					want = Counts{Recent: 1, Long: 1}
				}
				require.Equal(t, want, c.MemberCounts(key))
				require.Equal(t, old.started, c.Evidence(key).DeadSince)
			})
		}
	}
}

func TestClearMemoryPoolOutageBeforeAndAfterConclusion(t *testing.T) {
	for _, established := range []bool{false, true} {
		t.Run(map[bool]string{false: "Suspected", true: "Established"}[established], func(t *testing.T) {
			c, _, clock := newClockedCoordinator(t)
			a, b := pooledNode("A", "p", ""), pooledNode("B", "p", "")
			g := newRecorder("g", a, b)
			activeRegistration(c, memoryRecorder{g})
			c.finish(startManual(c, a, testCond), C.ProbeResult{}, nil)
			clock.Advance(time.Second)
			other := startManual(c, b, testCond)
			clock.Advance(time.Second)
			if !established {
				// The late second witness can still establish a suspicion across the clear.
				c.ClearMemory()
			}
			c.finish(other, C.ProbeResult{}, nil)
			require.Len(t, c.Outages(), 1)
			if !established {
				c.ClearMemory()
				require.Len(t, c.Outages(), 1)
			}
			clock.Advance(time.Second)
			c.NoteNewSuccess(b, clock.Now())
			require.Empty(t, c.Outages())
			require.Len(t, c.outages.past, 1)
			o := c.outages.past[0]
			require.Equal(t, OutageEstablished, o.state)
			require.False(t, o.committed)
			if established {
				c.ClearMemory()
				require.True(t, o.committed)
				require.False(t, o.counted)
			}
			clock.Advance(AttributionDelay)
			c.AttributeDue()
			require.True(t, o.committed)
			require.Equal(t, !established, o.counted)
			want := Counts{}
			if !established {
				want = Counts{Recent: 1, Long: 1}
			}
			require.Equal(t, want, c.PoolCounts("p", testCond))
			c.ClearMemory()
			require.Zero(t, c.PoolCounts("p", testCond))
		})
	}
}

func TestClearMemoryDelegatedFailureUsesProofTime(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	child := node("child")
	key := Key{child, testCond}
	owner := c.Register(memoryRecorder{newRecorder("child")})
	g := newRecorder("parent", child)
	activeRegistration(c, memoryRecorder{g})
	owner.Delegate(key, true)
	before := clock.Now()
	clock.Advance(time.Second)
	c.network.past = []interval{{start: before, end: clock.Now()}}
	c.ClearMemory()
	owner.Publish(key, Availability{State: Dead, Failed: true, Exec: 1, Proof: before})
	require.Equal(t, Dead, c.Evidence(key).State)
	require.Equal(t, before, c.Evidence(key).charged)
	require.True(t, c.Evidence(key).chargedLocal)
	require.False(t, lastCompletion(t, g, child).Charged)
	require.Empty(t, c.quality.pending)
	owner.Publish(key, Availability{State: Alive, Exec: 2, Proof: clock.Now()})
	owner.Publish(key, Availability{State: Dead, Failed: true, Exec: 3, Proof: clock.Now()})
	require.True(t, lastCompletion(t, g, child).Charged)
	clock.Advance(AttributionDelay)
	c.AttributeDue()
	require.Equal(t, Counts{Recent: 1, Long: 1}, c.MemberCounts(key))
}

func TestClearMemoryLeavesEarlierDegradationsToTheNotice(t *testing.T) {
	c, _, clock := newClockedCoordinator(t)
	a := pooledNode("A", "pool", "")
	g := newRecorder("g", a)
	activeRegistration(c, memoryRecorder{g})
	before := clock.Now()
	clock.Advance(time.Second)
	require.Equal(t, Attribution{Outcome: OutcomeMember}, c.Attribute(before, []string{"pool"}, testCond, false))
	require.Equal(t, MemoryClearResult{Clear: 1, Groups: 1}, c.ClearMemory())
	// a group still on a batch taken before the notice waits for the notice to drop it
	require.Equal(t, Attribution{Wait: true}, c.Attribute(before, []string{"pool"}, testCond, false))
	clock.Advance(time.Second)
	require.Equal(t, Attribution{Outcome: OutcomeMember}, c.Attribute(clock.Now(), []string{"pool"}, testCond, false))
}
