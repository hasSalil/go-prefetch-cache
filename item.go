package cache

type item struct {
	value      interface{}
	nonce      string
	generation int64
}
