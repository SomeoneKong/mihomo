package outbound

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

func testNinja(t *testing.T, dialerProxy string) *Ninja {
	t.Helper()
	option := NinjaOption{
		Name:         "ninja",
		Server:       "192.0.2.1",
		Port:         443,
		Method:       "aes-128-gcm",
		Password:     "test-user",
		NodePassword: "test-node",
	}
	option.DialerProxy = dialerProxy
	n, err := NewNinja(option)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestNinjaReportsItsDialerProxy(t *testing.T) {
	n := testNinja(t, "relay")
	if info := n.ProxyInfo(); info.DialerProxy != "relay" {
		t.Fatalf("dialer proxy %q", info.DialerProxy)
	}
	if !n.SupportUOT() {
		t.Fatal("UDP is carried over TCP")
	}
}

func TestNinjaFirstFrameFollowsTheDialContext(t *testing.T) {
	// Nobody reads the other end, so writing the first frame blocks.
	local, remote := net.Pipe()
	defer remote.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := testNinja(t, "").StreamConnContext(ctx, local, &C.Metadata{Host: "example.com", DstPort: 80})
	if err == nil {
		t.Fatal("the first frame was written to a peer that never reads")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("took %v to give up", elapsed)
	}
	if _, err := local.Write([]byte{0}); err == nil {
		t.Fatal("the connection was left open")
	}
}

func TestNinjaConnOutlivesTheDialContext(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	go io.Copy(io.Discard, remote)
	ctx, cancel := context.WithCancel(context.Background())
	conn, err := testNinja(t, "").StreamConnContext(ctx, local, &C.Metadata{Host: "example.com", DstPort: 80})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cancel()
	time.Sleep(50 * time.Millisecond)
	if _, err := conn.Write([]byte("after the dial")); err != nil {
		t.Fatalf("cancelling the dial context broke the connection: %v", err)
	}
}
