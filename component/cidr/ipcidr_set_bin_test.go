package cidr

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadIpCidrSetCorruptLength(t *testing.T) {
	set := NewIpCidrSet()
	assert.NoError(t, set.AddIpCidrForString("192.0.2.0/24"))
	assert.NoError(t, set.Merge())
	var valid bytes.Buffer
	assert.NoError(t, set.WriteBin(&valid))

	restored, err := ReadIpCidrSet(bytes.NewReader(valid.Bytes()))
	assert.NoError(t, err)
	assert.True(t, restored.IsContainForString("192.0.2.1"))
	assert.False(t, restored.IsContainForString("198.51.100.1"))

	corrupt := []byte{1}
	corrupt = binary.BigEndian.AppendUint64(corrupt, 1<<62)
	_, err = ReadIpCidrSet(bytes.NewReader(corrupt))
	assert.Error(t, err)
}
