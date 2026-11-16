package executor

import "sync/atomic"

var processManaged atomic.Bool

// ProcessManaged reports whether the program that started the core manages its executable
// and process, as the config in effect says: it upgrades and restarts the core itself, and
// a core replacing or restarting itself would run outside its control.
func ProcessManaged() bool {
	return processManaged.Load()
}
