package config

import (
	"math"
	"time"

	"github.com/metacubex/mihomo/component/probe"
	"github.com/metacubex/mihomo/log"
)

// parseProbeCensusHours checks the census interval of the on-demand groups: 0 for none, at
// least a day otherwise.
func parseProbeCensusHours(hours int) int {
	minimum := int(probe.MinCensusInterval.Hours())
	switch {
	case hours < 0:
		log.Warnln("probe-census-hours %d is negative, census disabled", hours)
		return 0
	case hours > 0 && hours < minimum:
		log.Warnln("probe-census-hours %d is below %d, using %d", hours, minimum, minimum)
		return minimum
	case int64(hours) > math.MaxInt64/int64(time.Hour):
		maximum := int(math.MaxInt64 / int64(time.Hour))
		log.Warnln("probe-census-hours %d is too large, using %d", hours, maximum)
		return maximum
	}
	return hours
}
