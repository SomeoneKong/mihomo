package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter/outbound"
)

// an address nothing listens on, so a test fails without leaving the host
const unreachableTestURL = "http://127.0.0.1:1/"

func TestURLTestCanceledIsNotRecorded(t *testing.T) {
	p := NewProxy(outbound.NewDirect())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.URLTest(ctx, unreachableTestURL, nil); err == nil {
		t.Fatal("expected the canceled test to fail")
	}
	if len(p.DelayHistory()) != 0 || !p.alive.Load() {
		t.Fatal("canceled test was recorded")
	}
}

func TestURLTestTimeoutIsRecorded(t *testing.T) {
	p := NewProxy(outbound.NewDirect())
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if _, err := p.URLTest(ctx, unreachableTestURL, nil); err == nil {
		t.Fatal("expected the timed out test to fail")
	}
	if len(p.DelayHistory()) != 1 || p.alive.Load() {
		t.Fatal("timed out test was not recorded as a failure")
	}
}
