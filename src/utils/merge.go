package utils

func Merge(x []string, y []string) (slice []string) {
	switch len(y) {
	case 0:
		return x
	default:
		slice = make([]string, len(x)+len(y))
		copy(slice, x)
		copy(slice[len(y):], y)
		return slice
	}
}
