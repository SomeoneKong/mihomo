package outboundgroup

import (
	"fmt"
	"strings"

	"github.com/dlclark/regexp2"
)

// checkFilters reports an invalid expression in the filters of a group, which are compiled
// without error checks later.
func checkFilters(option GroupCommonOption) error {
	for _, filter := range []struct{ name, value string }{
		{"filter", option.Filter},
		{"exclude-filter", option.ExcludeFilter},
	} {
		if filter.value == "" {
			continue
		}
		for _, expr := range strings.Split(filter.value, "`") {
			if _, err := regexp2.Compile(expr, regexp2.None); err != nil {
				return fmt.Errorf("invalid %s %q: %w", filter.name, expr, err)
			}
		}
	}
	return nil
}
