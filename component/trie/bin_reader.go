package trie

import (
	"io"

	"github.com/metacubex/mihomo/common/utils"
)

// readBytes reads n bytes, where n comes from the input: the result grows
// with the data actually read instead of being allocated for n upfront.
func readBytes(r io.Reader, n int64) ([]byte, error) {
	b := make([]byte, 0, utils.PreallocLen(n, 1))
	for int64(len(b)) < n {
		chunk := utils.PreallocLen(n-int64(len(b)), 1)
		b = append(b, make([]byte, chunk)...)
		if _, err := io.ReadFull(r, b[len(b)-chunk:]); err != nil {
			return nil, err
		}
	}
	return b, nil
}
