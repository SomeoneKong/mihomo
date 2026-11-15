package common

import (
	"bytes"
	"testing"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestUidWarnsOnlyWhenTheUidIsUnknown(t *testing.T) {
	uids, err := utils.NewUnsignedRanges[uint32]("1000")
	require.NoError(t, err)
	rule := &Uid{uids: uids, adapter: "DIRECT"}
	var output bytes.Buffer
	previous := logrus.StandardLogger().Out
	logrus.SetOutput(&output)
	defer logrus.SetOutput(previous)

	for _, uid := range []uint32{1000, 1001} {
		matched, _ := rule.Match(&C.Metadata{Uid: uid}, C.RuleMatchHelper{})
		require.Equal(t, uid == 1000, matched)
	}
	require.NotContains(t, output.String(), "could not get uid")

	matched, _ := rule.Match(&C.Metadata{}, C.RuleMatchHelper{})
	require.False(t, matched)
	require.Contains(t, output.String(), "could not get uid")
}
