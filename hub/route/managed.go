package route

import (
	"fmt"

	"github.com/metacubex/mihomo/hub/executor"
	"github.com/metacubex/mihomo/log"

	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

// refuseManaged answers a request to upgrade or restart the core while the program that
// started it manages its process, and reports whether it did.
func refuseManaged(w http.ResponseWriter, r *http.Request, action string) bool {
	if !executor.ProcessManaged() {
		return false
	}
	log.Warnln("refused to %s the core: its process is managed by the program that started it", action)
	render.Status(r, http.StatusForbidden)
	render.JSON(w, r, newError(fmt.Sprintf("the core's process is managed by the program that started it: %s it there", action)))
	return true
}
