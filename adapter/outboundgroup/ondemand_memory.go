package outboundgroup

import (
	"time"

	"github.com/metacubex/mihomo/component/probe"
	C "github.com/metacubex/mihomo/constant"
)

func (od *onDemand) clearMemory(boundary probe.MemoryBoundary, now time.Time) {
	od.memory = boundary
	fields := od.event("memoryCleared")
	fields["clear"] = boundary.Clear
	fields["pendingDegradations"] = len(od.degradations)
	fields["attributions"] = len(od.attributed)
	od.degradations = nil
	od.attributed = make(map[executionKey]heardAttribution)
	od.early, od.earlySuccesses = nil, nil
	standings, degradations, consumed, failedTrials := 0, 0, 0, 0
	for p, q := range od.quality {
		degradations += len(q.degradations)
		consumed += len(q.consumed)
		failedTrials += q.failedTrials
		delete(od.quality, p)
		if q.standing != standingNormal {
			var ended uint64
			if q.trial != nil {
				ended = q.trial.round
			}
			standing := od.standingFields(p, &memberQuality{}, od.countsOf(p, now), "cleared", ended)
			standing["previous"] = q.standing.String()
			od.record(standing)
			standings++
		}
	}
	fields["standings"], fields["degradations"] = standings, degradations
	fields["consumed"], fields["failedTrials"] = consumed, failedTrials
	cooldowns := 0
	for _, ms := range od.states {
		if ms.cooldown != 0 {
			cooldowns++
		}
		ms.cooldown, ms.cooldownExec = 0, 0
		ms.cooldownUntil, ms.cooldownReset = time.Time{}, time.Time{}
	}
	fields["cooldowns"] = cooldowns
	records := 0
	if ps := od.prefer; ps != nil {
		if ps.round != nil {
			od.endRound(now, "void", "cleared", nil)
		}
		if ps.preferred != nil {
			od.recordPreferenceEnd(ps.preferred, causePreferenceEnd, "cleared")
		}
		ps.since, ps.seen, ps.lastChange, ps.basis = time.Time{}, time.Time{}, time.Time{}, nil
		for key, pair := range ps.records {
			if key == pair.members {
				records += len(pair.results)
			}
		}
		ps.records = make(map[[2]C.Proxy]*pairRecords)
	}
	fields["comparisons"] = records
	fields["display"] = od.display("quality", probe.LevelInfo, probe.T("已清除本组的策略记忆", "Cleared this group's policy memory"))
	od.record(fields)
	od.computeOrder()
	od.reselect(now, selectionCause{Reason: causeMemoryCleared})
	od.setRoute(now, selectionCause{Reason: causeMemoryCleared})
}
