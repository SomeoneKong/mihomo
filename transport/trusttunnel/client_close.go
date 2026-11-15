package trusttunnel

import (
	"io"
	"time"

	"github.com/metacubex/mihomo/common/once"
)

// A closed client TCP conn lingers before it resets its stream: it waits for
// the transport to send what was written, then for the server to end its
// response, reading and dropping at most closeLingerBytes of it. A server
// drops what it has received but not yet relayed once the stream is reset.
const (
	closeLingerTimeout = 5 * time.Second
	closeLingerBytes   = 64 << 10
)

func (t *tcpConn) Close() error {
	if t.uploaded == nil {
		return t.httpConn.Close()
	}
	if !t.closed.CompareAndSwap(false, true) {
		return nil
	}
	var err error
	if closer, ok := t.writer.(io.Closer); ok {
		err = closer.Close()
	}
	go t.linger()
	return err
}

func (t *tcpConn) linger() {
	timer := time.NewTimer(closeLingerTimeout)
	defer timer.Stop()
	select {
	case <-t.uploaded:
	case <-timer.C:
		_ = t.httpConn.Close()
		return
	}
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		if t.waitCreated() == nil {
			_, _ = io.CopyN(io.Discard, t.body, closeLingerBytes)
		}
	}()
	select {
	case <-drained:
	case <-timer.C:
	}
	_ = t.httpConn.Close() // also ends the read above
}

// newUploadSignal returns the channel for httpConn.uploaded and the function
// that closes it, which may be called more than once.
func newUploadSignal() (<-chan struct{}, func()) {
	uploaded := make(chan struct{})
	return uploaded, once.OnceFunc(func() { close(uploaded) })
}
