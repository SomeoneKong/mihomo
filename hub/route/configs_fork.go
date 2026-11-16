package route

import "github.com/metacubex/mihomo/tunnel/statistic"

// forkConfigSchema is the part of a config patch for settings only this fork has.
type forkConfigSchema struct {
	ConnectionStats *bool `json:"connection-stats"`
}

func (s forkConfigSchema) apply() {
	if s.ConnectionStats != nil {
		statistic.DefaultConnStats.SetEnabled(*s.ConnectionStats)
	}
}
