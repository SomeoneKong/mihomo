package trie

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

// hugeLength is far more than the input holds and than could be allocated.
const hugeLength = int64(1) << 62

func TestReadDomainSetBinCorruptLength(t *testing.T) {
	var b DomainSetBuilder
	assert.NoError(t, b.Insert("example.com"))
	var valid bytes.Buffer
	assert.NoError(t, b.Build().WriteBin(&valid))

	restored, err := ReadDomainSetBin(bytes.NewReader(valid.Bytes()))
	assert.NoError(t, err)
	assert.True(t, restored.Has("example.com"))

	corruptLeaves := []byte{1}
	corruptLeaves = binary.BigEndian.AppendUint64(corruptLeaves, uint64(hugeLength))
	_, err = ReadDomainSetBin(bytes.NewReader(corruptLeaves))
	assert.Error(t, err)

	corruptLabels := []byte{1}
	for i := 0; i < 2; i++ { // leaves, labelBitmap
		corruptLabels = binary.BigEndian.AppendUint64(corruptLabels, 1)
		corruptLabels = binary.BigEndian.AppendUint64(corruptLabels, 0)
	}
	corruptLabels = binary.BigEndian.AppendUint64(corruptLabels, uint64(hugeLength))
	_, err = ReadDomainSetBin(bytes.NewReader(corruptLabels))
	assert.Error(t, err)
}

func TestReadDomainMapBinCorruptLength(t *testing.T) {
	var b DomainSetBuilder
	assert.NoError(t, b.Insert("example.com"))
	var data bytes.Buffer
	assert.NoError(t, b.Build().WriteBin(&data))
	data.Write([]byte{0, 1}) // separator, version
	assert.NoError(t, binary.Write(&data, binary.BigEndian, hugeLength))

	_, err := ReadDomainMapBin(&data, func(r io.Reader) (value int64, err error) {
		err = binary.Read(r, binary.BigEndian, &value)
		return
	})
	assert.Error(t, err)
}

func TestReadBytesBeyondPrealloc(t *testing.T) {
	data := make([]byte, 5<<20)
	data[len(data)-1] = 1

	got, err := readBytes(bytes.NewReader(data), int64(len(data)))
	assert.NoError(t, err)
	assert.Equal(t, data, got)

	_, err = readBytes(bytes.NewReader(data), int64(len(data))+1)
	assert.True(t, errors.Is(err, io.ErrUnexpectedEOF), err)
}
