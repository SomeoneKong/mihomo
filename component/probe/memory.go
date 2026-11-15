package probe

import "time"

// SettleWait bounds the wait for a group to publish its state on reload or memory clear.
const SettleWait = 500 * time.Millisecond

// MemoryBoundary identifies the latest clear within this coordinator's lifetime.
type MemoryBoundary struct {
	Clear uint64
	At    time.Time
}

// MemoryCleared is queued in the same order as probe completions and attributions.
type MemoryCleared struct {
	MemoryBoundary
	name string
	done chan<- clearAck
}

func (MemoryCleared) notice() {}

type clearAck struct {
	name string
	at   time.Time
}

// Processed acknowledges that this instance has reselected and published its state.
func (m MemoryCleared) Processed() { m.done <- clearAck{name: m.name, at: time.Now()} }

// MemoryClearResult counts a name once all its registered instances have acknowledged.
type MemoryClearResult struct {
	Clear   uint64 `json:"clear"`
	Groups  int    `json:"groups"`
	Pending int    `json:"pending"`
}

// MemoryBoundary returns the boundary against which to check a reload handoff.
func (c *Coordinator) MemoryBoundary() MemoryBoundary {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.memory
}

// ClearMemory clears the shared and group policy memories and reports the groups reached.
func (c *Coordinator) ClearMemory() MemoryClearResult {
	c.clearMutex.Lock()
	defer c.clearMutex.Unlock()

	c.mutex.Lock()
	c.memory = MemoryBoundary{Clear: c.memory.Clear + 1, At: c.now()}
	c.clearQualityLocked()
	result := MemoryClearResult{Clear: c.memory.Clear}
	remaining := make(map[string]int)
	done := make(chan clearAck, len(c.registrations)+1)
	for _, r := range c.registrations {
		if !r.persistent {
			name := r.group.Name()
			remaining[name]++
			r.group.Deliver(MemoryCleared{MemoryBoundary: c.memory, name: name, done: done})
		}
	}
	// The window starts with the notices sent, however late this goroutine runs on: a reply
	// later than it leaves its group pending.
	deadline := time.Now().Add(SettleWait)
	c.mutex.Unlock()

	timer := time.AfterFunc(time.Until(deadline), func() { done <- clearAck{} })
	defer timer.Stop()
	for len(remaining) > 0 {
		ack := <-done
		if ack.at.IsZero() {
			break
		}
		if ack.at.After(deadline) {
			continue
		}
		remaining[ack.name]--
		if remaining[ack.name] == 0 {
			delete(remaining, ack.name)
			result.Groups++
		}
	}
	result.Pending = len(remaining)
	message := Tf("已清除 %d 个组的策略记忆", "Cleared policy memory for %d groups", result.Groups)
	if result.Pending > 0 {
		message = Tf("已清除 %d 个组的策略记忆，%d 个组稍后完成", "Cleared policy memory for %d groups, %d groups pending", result.Groups, result.Pending)
	}
	c.records.appendPerformance(map[string]any{
		"event": "memoryClearDone", "clear": result.Clear, "groups": result.Groups, "pending": result.Pending,
		"display": Display("quality", LevelInfo, nil, nil, message),
	})
	return result
}

func (c *Coordinator) clearQualityLocked() {
	q := c.quality
	failures, pools, outages := 0, 0, 0
	for _, events := range q.failures {
		failures += len(events)
	}
	for _, events := range q.pools {
		pools += len(events)
	}
	q.failures = make(map[Key][]QualityEvent)
	q.pools = make(map[poolKey][]QualityEvent)
	pending := len(q.pending)
	for _, p := range q.pending {
		if c.network.covers(p.started) {
			c.markLocalFailureLocked(p)
		}
		c.endAttributionLocked(p, Attribution{Outcome: OutcomeUndecided}, "cleared")
	}
	q.pending = nil
	for _, o := range c.outages.past {
		if o.state == OutageEstablished && !o.committed {
			o.committed = true
			c.recordOutageLocked(o, "cleared", nil)
			c.deliverOutageLocked(o)
			outages++
		}
	}
	c.armQualityLocked()
	c.records.appendPerformance(map[string]any{
		"event": "memoryCleared", "source": "api", "clear": c.memory.Clear, "at": formatTime(c.memory.At),
		"failures": failures, "poolEvents": pools, "pendingFailures": pending, "outages": outages,
		"display": Display("quality", LevelInfo, nil, nil, T("已受理清除策略记忆，共享计次已清除", "Policy memory clear accepted, shared counts cleared")),
	})
}
