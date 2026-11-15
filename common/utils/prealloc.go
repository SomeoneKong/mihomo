package utils

// maxPreallocBytes bounds the memory allocated for a length read from
// untrusted input before the data it announces has been read.
const maxPreallocBytes = 4 << 20

// PreallocLen returns how many of length elements, each size bytes, to
// allocate upfront when length comes from untrusted input. A corrupt length
// then fails at the end of the input instead of exhausting memory, while
// ordinary lengths are still allocated at their exact size.
func PreallocLen(length int64, size int) int {
	if size < 1 {
		size = 1
	}
	if limit := int64(maxPreallocBytes / size); length > limit {
		return int(limit)
	}
	if length < 0 {
		return 0
	}
	return int(length)
}
