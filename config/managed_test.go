package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAConfigTellsTheCoreItsProcessIsManaged(t *testing.T) {
	raw, err := UnmarshalRawConfig([]byte("process-managed: true\n"))
	require.NoError(t, err)
	require.True(t, raw.ProcessManaged)
	raw, err = UnmarshalRawConfig([]byte("mixed-port: 7890\n"))
	require.NoError(t, err)
	require.False(t, raw.ProcessManaged, "a core runs on its own unless told")
}
