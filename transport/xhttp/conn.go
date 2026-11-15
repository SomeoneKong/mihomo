package xhttp

import (
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/httputils"
	"github.com/metacubex/mihomo/common/once"
)

// A closed client conn lingers before it resets its streams: it waits for the
// transport to send what was written, then for the server to end the
// download, reading and dropping at most closeLingerBytes of it. A server
// drops what it has received but not yet relayed once the stream is reset.
const (
	closeLingerTimeout = 5 * time.Second
	closeLingerBytes   = 64 << 10
)

type Conn struct {
	writer  io.WriteCloser
	reader  io.ReadCloser
	onClose func()
	httputils.NetAddr

	// uploaded, set on client conns, is closed once the transport is done
	// with the request body that carries the upload.
	uploaded <-chan struct{}
	closed   atomic.Bool

	// deadlines
	deadline *time.Timer
}

func (c *Conn) Write(b []byte) (int, error) {
	return c.writer.Write(b)
}

func (c *Conn) Read(b []byte) (int, error) {
	if c.closed.Load() {
		return 0, net.ErrClosed
	}
	return c.reader.Read(b)
}

func (c *Conn) Close() error {
	if c.uploaded == nil {
		// A server conn closes its request body on every call: the handler
		// closes it again before it returns, so that the server reads the next
		// request of the connection only after the body is closed.
		return errors.Join(c.writer.Close(), c.closeStreams())
	}
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	err := c.writer.Close()
	go c.linger()
	return err
}

func (c *Conn) linger() {
	defer func() { _ = c.closeStreams() }() // also ends a read below
	timer := time.NewTimer(closeLingerTimeout)
	defer timer.Stop()
	wait := func(ch <-chan struct{}) bool {
		select {
		case <-ch:
			return true
		case <-timer.C:
			return false
		}
	}
	if !wait(c.uploaded) {
		return
	}
	download, ok := c.reader.(*WaitReadCloser)
	if !ok || !wait(download.wait) {
		return
	}
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		_, _ = io.CopyN(io.Discard, c.reader, closeLingerBytes)
	}()
	wait(drained)
}

func (c *Conn) closeStreams() error {
	err := c.reader.Close()
	if c.onClose != nil {
		c.onClose()
	}
	return err
}

// newUploadSignal returns the channel for Conn.uploaded and the function
// that closes it, which may be called more than once.
func newUploadSignal() (<-chan struct{}, func()) {
	uploaded := make(chan struct{})
	return uploaded, once.OnceFunc(func() { close(uploaded) })
}

func (c *Conn) SetReadDeadline(t time.Time) error  { return c.SetDeadline(t) }
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.SetDeadline(t) }

func (c *Conn) SetDeadline(t time.Time) error {
	if t.IsZero() {
		if c.deadline != nil {
			c.deadline.Stop()
			c.deadline = nil
		}
		return nil
	}
	d := time.Until(t)
	if c.deadline != nil {
		c.deadline.Reset(d)
		return nil
	}
	c.deadline = time.AfterFunc(d, func() {
		c.Close()
	})
	return nil
}
