package executor

import "github.com/metacubex/mihomo/tunnel/statistic"

// updateConnectionStats starts or stops counting connections as the config says.
func updateConnectionStats(enabled bool) {
	statistic.DefaultConnStats.SetEnabled(enabled)
}

func connectionStatsEnabled() bool {
	return statistic.DefaultConnStats.Enabled()
}
