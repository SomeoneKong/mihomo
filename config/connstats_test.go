package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAConfigEnablesConnectionStats(t *testing.T) {
	raw, err := UnmarshalRawConfig([]byte("connection-stats: true\n"))
	require.NoError(t, err)
	require.True(t, raw.ConnectionStats)
	raw, err = UnmarshalRawConfig([]byte("mixed-port: 7890\n"))
	require.NoError(t, err)
	require.False(t, raw.ConnectionStats, "connections are not counted unless asked")
}
