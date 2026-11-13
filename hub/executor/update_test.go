package executor

import (
	"runtime"
	"sync"
	"testing"
)

func waitingUpdate() chan struct{} {
	updateMu.Lock()
	defer updateMu.Unlock()
	return updateTail
}

func TestUpdatesGoInTheOrderTheyCome(t *testing.T) {
	unlock := LockUpdate()
	var (
		mu    sync.Mutex
		order []int
		wg    sync.WaitGroup
	)
	for i := 1; i <= 5; i++ {
		i := i
		tail := waitingUpdate()
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := LockUpdate()
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			unlock()
		}()
		for waitingUpdate() == tail { // until it queued up
			runtime.Gosched()
		}
	}
	unlock()
	wg.Wait()
	for i, n := range order {
		if n != i+1 {
			t.Fatalf("updates went in the order %v", order)
		}
	}
}
