//go:build with_gvisor && !no_tailscale

package outbound

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// The tests never start a server: takeOverState stands in for the start of the
// outbounds that would run one, and start is only called where it must fail early.

func newTestTailscale(t *testing.T, name string, stateDir string) *Tailscale {
	t.Helper()
	ts, err := NewTailscale(TailscaleOption{Name: name, StateDir: stateDir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	return ts
}

func tailscaleClosed(ts *Tailscale) bool {
	return ts.ctx.Err() != nil
}

func tailscaleStateOwner(ts *Tailscale) *Tailscale {
	tailscaleServersMutex.Lock()
	defer tailscaleServersMutex.Unlock()
	return tailscaleServers[ts.stateKey()]
}

func TestTailscaleTakeOverState(t *testing.T) {
	// the same proxy parsed by three config reloads
	old := newTestTailscale(t, "ts", "ts-handover")
	late := newTestTailscale(t, "ts", "ts-handover")
	current := newTestTailscale(t, "ts", "ts-handover")

	if err := old.takeOverState(); err != nil {
		t.Fatal(err)
	}
	if err := current.takeOverState(); err != nil {
		t.Fatal(err)
	}
	if !tailscaleClosed(old) {
		t.Fatal("outdated outbound was not closed")
	}

	// an outdated outbound started later must not take the state dir back
	if err := late.start(); !errors.Is(err, errTailscaleOutdated) {
		t.Fatalf("expected %v, got %v", errTailscaleOutdated, err)
	}
	if late.serverStarted {
		t.Fatal("outdated outbound started its server")
	}
	if tailscaleClosed(current) || tailscaleStateOwner(current) != current {
		t.Fatal("current outbound lost the state dir")
	}

	_ = current.Close()
	if tailscaleStateOwner(current) != nil {
		t.Fatal("closed outbound still holds its state dir")
	}
}

func TestTailscaleTakeOverStateOfRenamedProxy(t *testing.T) {
	// the state dir is the node, whatever the proxy is called
	old := newTestTailscale(t, "ts-a", "ts-handover-renamed")
	current := newTestTailscale(t, "ts-b", "ts-handover-renamed")

	if err := old.takeOverState(); err != nil {
		t.Fatal(err)
	}
	if err := current.takeOverState(); err != nil {
		t.Fatal(err)
	}
	if !tailscaleClosed(old) {
		t.Fatal("outbound on the same state dir was not closed")
	}
}

func TestTailscaleKeepServerOfOtherStateDir(t *testing.T) {
	a := newTestTailscale(t, "ts", "ts-handover-a")
	b := newTestTailscale(t, "ts", "ts-handover-b")

	if err := a.takeOverState(); err != nil {
		t.Fatal(err)
	}
	if err := b.takeOverState(); err != nil {
		t.Fatal(err)
	}
	if tailscaleClosed(a) || tailscaleClosed(b) {
		t.Fatal("outbound on another state dir was closed")
	}
}

func TestTailscaleConcurrentHandover(t *testing.T) {
	// outbounds of successive reloads taking over the state dir at the same time
	const n = 8
	outbounds := make([]*Tailscale, n)
	for i := range outbounds {
		outbounds[i] = newTestTailscale(t, "ts", "ts-handover-concurrent")
	}

	tookOver := make([]bool, n)
	var wg sync.WaitGroup
	for i, ts := range outbounds {
		i, ts := i, ts
		wg.Add(1)
		go func() {
			defer wg.Done()
			tookOver[i] = ts.takeOverState() == nil
		}()
	}
	wg.Wait()

	var running []*Tailscale
	for i, ts := range outbounds {
		if tookOver[i] && !tailscaleClosed(ts) {
			running = append(running, ts)
		}
	}
	if len(running) != 1 || tailscaleStateOwner(running[0]) != running[0] {
		t.Fatalf("%d outbounds hold the state dir, expected exactly the owner", len(running))
	}
}

func TestTailscaleDialGivesWayToClose(t *testing.T) {
	// the server dials through withLifetime, so a start stuck on a dial gives way
	ts := newTestTailscale(t, "ts", "ts-handover-dial")
	ctx, cancel := ts.withLifetime(context.Background())
	defer cancel()
	if ctx.Err() != nil {
		t.Fatal("dial context canceled while the outbound is open")
	}
	_ = ts.Close()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("dial context not canceled when the outbound was closed")
	}
}

func TestTailscaleCanceledCallerDoesNotTakeOver(t *testing.T) {
	// e.g. the health check of a provider a new config replaced
	ts := newTestTailscale(t, "ts", "ts-handover-canceled")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ts.ensureStarted(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected %v, got %v", context.Canceled, err)
	}
	if tailscaleStateOwner(ts) != nil {
		t.Fatal("canceled caller took the state dir over")
	}
}

func TestTailscaleClosedOutboundStaysDown(t *testing.T) {
	ts := newTestTailscale(t, "ts", "ts-handover-closed")
	_ = ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ts.ensureStarted(ctx); !errors.Is(err, errTailscaleClosed) {
		t.Fatalf("expected %v, got %v", errTailscaleClosed, err)
	}
	if ts.serverStarted || tailscaleStateOwner(ts) != nil {
		t.Fatal("closed outbound started its server")
	}
}

func TestTailscaleFailedStartIsNotReused(t *testing.T) {
	old := newTestTailscale(t, "ts", "ts-failed-start")
	current := newTestTailscale(t, "ts", "ts-failed-start")
	if err := current.takeOverState(); err != nil {
		t.Fatal(err)
	}
	// a start is not retried, so only a new outbound can recover
	if err := old.start(); err == nil {
		t.Fatal("outdated outbound started")
	}
	if IsReusable(old) || !IsReusable(current) {
		t.Fatal("outbound that failed to start is reused, or one that didn't is not")
	}
}
