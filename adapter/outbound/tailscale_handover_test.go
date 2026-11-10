//go:build with_gvisor && !no_tailscale

package outbound

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

// The tests never start a server: takeOverState stands in for the start of the
// outbounds that would run one, and start is only called where it must fail early.

var testHomes sync.Map // *testing.T -> its home dir

// useTestHome points the home dir, which state dirs are resolved in, to a dir of the
// test, as taking a state dir over creates it.
func useTestHome(t *testing.T) string {
	t.Helper()
	if home, ok := testHomes.Load(t); ok {
		return home.(string)
	}
	previous := C.Path.HomeDir()
	home := t.TempDir()
	C.SetHomeDir(home)
	testHomes.Store(t, home)
	t.Cleanup(func() {
		C.SetHomeDir(previous)
		testHomes.Delete(t)
	})
	return home
}

func newTestTailscale(t *testing.T, name string, stateDir string) *Tailscale {
	t.Helper()
	useTestHome(t)
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
	return tailscaleServers[filepath.Clean(ts.option.StateDir)]
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

func TestTailscaleWaitForAStartGivesWayToContext(t *testing.T) {
	ts := newTestTailscale(t, "ts", "ts-start-in-progress")
	// another caller is starting it, and that start waits on this caller
	ts.startClaimed.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := ts.ensureStarted(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected %v, got %v", context.DeadlineExceeded, err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("waited %v for the start of another caller", elapsed)
	}
}

// otherSpellings returns other spellings of dir that this system has: through a
// symbolic link to its parent, and in other case on a case-insensitive file system.
func otherSpellings(t *testing.T, dir string) []string {
	t.Helper()
	home := useTestHome(t)
	var spellings []string
	if err := os.Symlink(filepath.Dir(filepath.Join(home, dir)), filepath.Join(home, "link")); err == nil {
		spellings = append(spellings, filepath.Join("link", filepath.Base(dir)))
	}
	probe := filepath.Join(home, "case-probe")
	if err := os.Mkdir(probe, 0o700); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(filepath.Join(home, "CASE-PROBE")); err == nil && info.IsDir() {
		spellings = append(spellings, strings.ToUpper(dir))
	}
	if len(spellings) == 0 {
		t.Skip("no other spelling of a dir on this system")
	}
	return spellings
}

func TestTailscaleTakeOverStateSpelledOtherwise(t *testing.T) {
	// the state dir doesn't exist yet: neither outbound has started
	for _, spelling := range otherSpellings(t, filepath.Join("states", "ts-state")) {
		_ = os.RemoveAll(filepath.Join(C.Path.HomeDir(), "states", "ts-state"))
		old := newTestTailscale(t, "ts", filepath.Join("states", "ts-state"))
		current := newTestTailscale(t, "ts-renamed", spelling)
		if err := old.takeOverState(); err != nil {
			t.Fatal(err)
		}
		if err := current.takeOverState(); err != nil {
			t.Fatal(err)
		}
		if !tailscaleClosed(old) || tailscaleStateOwner(current) != current || tailscaleStateOwner(old) != nil {
			t.Fatalf("%s: the outbound on the same state dir was not taken over", spelling)
		}
		// an older outbound started later doesn't take it back under its spelling
		late := newTestTailscale(t, "ts", filepath.Join("states", "ts-state"))
		late.serial = old.serial
		if err := late.takeOverState(); !errors.Is(err, errTailscaleOutdated) {
			t.Fatalf("%s: expected %v, got %v", spelling, errTailscaleOutdated, err)
		}
		_ = current.Close()
		if tailscaleStateOwner(current) != nil {
			t.Fatalf("%s: the state dir is still held by a closed outbound", spelling)
		}
	}
}

func TestTailscaleStateDirOutlivesTheSpellingOfThePredecessor(t *testing.T) {
	home := useTestHome(t)
	if err := os.Mkdir(filepath.Join(home, "real"), 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(home, "link")
	if err := os.Symlink(filepath.Join(home, "real"), link); err != nil {
		t.Skip("no symbolic links on this system")
	}
	first := newTestTailscale(t, "ts", filepath.Join("link", "state"))
	second := newTestTailscale(t, "ts", filepath.Join("real", "state"))
	third := newTestTailscale(t, "ts", filepath.Join("real", "state"))
	if err := first.takeOverState(); err != nil {
		t.Fatal(err)
	}
	if err := second.takeOverState(); err != nil {
		t.Fatal(err)
	}
	// the spelling of the first is gone, the dir is not
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := third.takeOverState(); err != nil {
		t.Fatal(err)
	}
	if !tailscaleClosed(second) || tailscaleStateOwner(third) != third {
		t.Fatal("the state dir was not taken over from the outbound holding it")
	}
	_ = first.Close()
	_ = second.Close()
	if tailscaleStateOwner(third) != third {
		t.Fatal("closing an older outbound released the state dir of a newer one")
	}
	_ = third.Close()
	tailscaleServersMutex.Lock()
	defer tailscaleServersMutex.Unlock()
	for key, owner := range tailscaleServers {
		if owner == first || owner == second || owner == third {
			t.Fatalf("%s is still held", key)
		}
	}
}

func TestTailscaleFirstCallerGivesWayToContext(t *testing.T) {
	old := newTestTailscale(t, "ts", "ts-first-caller")
	if err := old.takeOverState(); err != nil {
		t.Fatal(err)
	}
	// a start of the older outbound is in progress, so taking over waits for it
	entered, release := make(chan struct{}), make(chan struct{})
	go old.startOnce.Do(func() {
		close(entered)
		<-release
	})
	<-entered
	current := newTestTailscale(t, "ts", "ts-first-caller")
	var releaseOnce sync.Once
	releaseOld := func() { releaseOnce.Do(func() { close(release) }) }
	// runs before the outbounds are closed, so a failed check doesn't leave them waiting
	t.Cleanup(func() {
		current.cancel()
		releaseOld()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- current.ensureStarted(ctx) }()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected %v, got %v", context.DeadlineExceeded, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the first caller kept waiting for the start")
	}
	// the start goes on without it; closed before it gets to start a server
	current.cancel()
	releaseOld()
	select {
	case <-current.startDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the start did not go on")
	}
	if !errors.Is(current.startErr, errTailscaleClosed) || !tailscaleClosed(old) || tailscaleStateOwner(current) != current {
		t.Fatalf("the start did not take over: %v", current.startErr)
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

func TestTailscaleRegistersDNSOnceInUse(t *testing.T) {
	ts := newTestTailscale(t, "ts", "ts-dns")
	if ts.unregisterDNSResolver != nil {
		t.Fatal("registered its DNS before it was in use")
	}
	ts.published()
	if ts.unregisterDNSResolver == nil {
		t.Fatal("did not register its DNS once in use")
	}
	_ = ts.Close()
	closed := newTestTailscale(t, "ts", "ts-dns")
	_ = closed.Close()
	closed.published()
	if closed.unregisterDNSResolver != nil {
		t.Fatal("a closed outbound registered its DNS")
	}
}
