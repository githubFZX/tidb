package hashtable

const (
	offset64 uint64 = 14695981039346656037
	prime64         = 1099511628211
)

// fnvHash64 is ported from go library, which is thread-safe.
func FnvHash64(data []byte) uint64 {
	hash := offset64
	for _, c := range data {
		hash *= prime64
		hash ^= uint64(c)
	}
	return hash
}
