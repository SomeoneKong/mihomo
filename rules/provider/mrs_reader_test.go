package provider

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
)

func TestRulesMrsParseCorruptExtraLength(t *testing.T) {
	strategy := NewDomainStrategy()
	var body bytes.Buffer
	body.Write(MrsMagicBytes[:])
	body.WriteByte(strategy.Behavior().Byte())
	assert.NoError(t, binary.Write(&body, binary.BigEndian, int64(0)))     // count
	assert.NoError(t, binary.Write(&body, binary.BigEndian, int64(1)<<62)) // extra, without its data

	var compressed bytes.Buffer
	encoder, err := zstd.NewWriter(&compressed)
	assert.NoError(t, err)
	_, err = encoder.Write(body.Bytes())
	assert.NoError(t, err)
	assert.NoError(t, encoder.Close())

	_, err = rulesMrsParse(compressed.Bytes(), strategy)
	assert.Error(t, err)
}
