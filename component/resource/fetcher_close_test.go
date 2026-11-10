package resource

import (
	"context"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	P "github.com/metacubex/mihomo/constant/provider"
)

type memoryVehicle struct{ buf []byte }

func (v *memoryVehicle) Read(context.Context, utils.HashType) ([]byte, utils.HashType, error) {
	return v.buf, utils.MakeHash(v.buf), nil
}
func (v *memoryVehicle) Write([]byte) error { return nil }
func (v *memoryVehicle) Path() string       { return "" }
func (v *memoryVehicle) Url() string        { return "" }
func (v *memoryVehicle) Proxy() string      { return "" }
func (v *memoryVehicle) Type() P.VehicleType {
	return P.Compatible
}

func TestClosedFetcherDoesNotPublish(t *testing.T) {
	parsed, published := 0, 0
	parser := func(buf []byte) (string, error) {
		parsed++
		return string(buf), nil
	}
	f := NewFetcher("test", 0, &memoryVehicle{buf: []byte("updated")}, nil, parser, func(string) { published++ })
	_ = f.Close()

	// an update queued or still reading when the fetcher was closed
	if _, _, err := f.Update(); err == nil {
		t.Fatal("closed fetcher accepted an update")
	}
	if _, _, err := f.SideUpdate([]byte("side")); err == nil {
		t.Fatal("closed fetcher accepted a side update")
	}
	if parsed != 0 || published != 0 {
		t.Fatalf("closed fetcher parsed %d and published %d updates", parsed, published)
	}
}

func TestFetcherCloseWaitsForUpdateInProgress(t *testing.T) {
	parsing, resume := make(chan struct{}), make(chan struct{})
	published := false
	parser := func(buf []byte) (string, error) {
		close(parsing)
		<-resume
		return string(buf), nil
	}
	f := NewFetcher("test", 0, &memoryVehicle{buf: []byte("updated")}, nil, parser, func(string) { published = true })

	updated := make(chan error, 1)
	go func() {
		_, _, err := f.Update()
		updated <- err
	}()
	<-parsing
	closed := make(chan struct{})
	go func() {
		_ = f.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while an update was still parsing")
	case <-time.After(100 * time.Millisecond):
	}

	close(resume)
	<-closed
	if err := <-updated; err == nil || published {
		t.Fatal("update that was parsing when the fetcher was closed got published")
	}
}
