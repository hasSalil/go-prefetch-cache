package cache

type item struct {
	value      interface{}
	generation int64
}
