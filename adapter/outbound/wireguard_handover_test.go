package outbound

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func wireGuardSlotOwner(w *WireGuard) *WireGuard {
	wireGuardDevicesMutex.Lock()
	defer wireGuardDevicesMutex.Unlock()
	return wireGuardDevices[w.deviceKey()].owner
}

func wireGuardDeviceRunning(w *WireGuard) bool {
	return w.device != nil && !wireGuardDeviceClosed(w)
}

func newTestWireGuardOption(name string, port int) WireGuardOption {
	option := WireGuardOption{
		Name:       name,
		Ip:         "10.9.0.2",
		PrivateKey: "4F7zVRZpPEAHIEFBcZj4NITwyiXwzHHbRMpRlZr9bnk=",
		UDP:        true,
	}
	option.Server = "127.0.0.1"
	option.Port = port
	option.PublicKey = "rsPXORPczwaVYfkc29uC6p2Vcjh2POFmL+quiQbBezk="
	return option
}

func newTestWireGuardFromOption(t *testing.T, option WireGuardOption) *WireGuard {
	t.Helper()
	w, err := NewWireGuard(option)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func TestWireGuardTakeOverDeviceAfterPeerChange(t *testing.T) {
	// a reload that moves the proxy to another endpoint still replaces the old device
	old := newTestWireGuard(t, "wg", listenTestPeer(t))
	current := newTestWireGuard(t, "wg", listenTestPeer(t))

	if err := initTestWireGuard(t, old); err != nil {
		t.Fatal(err)
	}
	if err := initTestWireGuard(t, current); err != nil {
		t.Fatal(err)
	}
	if !wireGuardDeviceClosed(old) {
		t.Fatal("device of the outdated outbound is still running")
	}
}

func TestWireGuardKeepDeviceWhenReplacementFails(t *testing.T) {
	port := listenTestPeer(t)
	old := newTestWireGuard(t, "wg", port)
	if err := initTestWireGuard(t, old); err != nil {
		t.Fatal(err)
	}

	// accepted when parsed, rejected by the device setup
	option := newTestWireGuardOption("wg", port)
	option.PersistentKeepalive = -1
	broken := newTestWireGuardFromOption(t, option)
	if err := initTestWireGuard(t, broken); err == nil {
		t.Fatal("expected the setup of the replacement to fail")
	}
	if wireGuardDeviceClosed(old) {
		t.Fatal("device of the older outbound was closed by a failed replacement")
	}
	if wireGuardSlotOwner(old) != old {
		t.Fatal("failed replacement took the device slot")
	}
}

func TestWireGuardTakeOverWaitGivesWayToContext(t *testing.T) {
	port := listenTestPeer(t)
	old := newTestWireGuard(t, "wg", port)
	current := newTestWireGuard(t, "wg", port)
	if err := initTestWireGuard(t, old); err != nil {
		t.Fatal(err)
	}

	// the older device can't be closed yet, like one whose dial waits for this init
	old.initMutex.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	err := current.init(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected the takeover to give way to ctx, got %v", err)
	}
	if current.device != nil || !wireGuardDeviceRunning(old) {
		t.Fatal("device came up before the older one was down")
	}

	// once the older device can be closed, a later init completes the takeover
	old.initMutex.Unlock()
	if err := initTestWireGuard(t, current); err != nil {
		t.Fatal(err)
	}
	if wireGuardDeviceRunning(old) || !wireGuardDeviceRunning(current) || wireGuardSlotOwner(current) != current {
		t.Fatal("takeover did not complete")
	}
}

func TestWireGuardConcurrentHandover(t *testing.T) {
	// outbounds of successive reloads starting and closing at the same time
	port := listenTestPeer(t)
	const n = 8
	outbounds := make([]*WireGuard, n)
	for i := range outbounds {
		outbounds[i] = newTestWireGuard(t, "wg", port)
	}

	errs := make([]error, n)
	var wg sync.WaitGroup
	for i, w := range outbounds {
		i, w := i, w
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = initTestWireGuard(t, w)
			if i%3 == 0 {
				_ = w.Close()
			}
		}()
	}
	wg.Wait()

	// the newest outbound, which is not closed, must end up as the one running
	newest := outbounds[n-1]
	if errs[n-1] != nil {
		t.Fatalf("newest outbound failed to start: %v", errs[n-1])
	}
	for _, w := range outbounds {
		if w != newest && wireGuardDeviceRunning(w) {
			t.Fatal("device of an older outbound is still running")
		}
	}
	if !wireGuardDeviceRunning(newest) || wireGuardSlotOwner(newest) != newest {
		t.Fatal("newest outbound is not the one running")
	}
}

func TestWireGuardClosedOutboundStaysDown(t *testing.T) {
	w := newTestWireGuard(t, "wg", listenTestPeer(t))
	_ = w.Close()
	if err := initTestWireGuard(t, w); err == nil {
		t.Fatal("closed outbound brought up its device")
	}
	if w.device != nil {
		t.Fatal("closed outbound created a device")
	}
	wireGuardDevicesMutex.Lock()
	_, exist := wireGuardDevices[w.deviceKey()]
	wireGuardDevicesMutex.Unlock()
	if exist {
		t.Fatal("closed outbound holds its device key")
	}
}
