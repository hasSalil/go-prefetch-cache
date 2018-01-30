package cache

type Item struct {
	Value      interface{}
	Generation int64
}
