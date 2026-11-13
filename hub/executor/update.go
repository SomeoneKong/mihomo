package executor

import "sync"

var (
	updateMu   sync.Mutex
	updateTail = closedChan()
)

func closedChan() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}

// LockUpdate holds off other updates of the config until unlock is called, and lets
// them through in the order they called it. An update holds it from parsing to
// applying: parsing takes over what the running config can share, and updates that
// overlapped would otherwise apply in the order they finished parsing, which can put
// an older config over a newer one.
func LockUpdate() (unlock func()) {
	next := make(chan struct{})
	updateMu.Lock()
	previous := updateTail
	updateTail = next
	updateMu.Unlock()
	<-previous
	return func() { close(next) }
}
