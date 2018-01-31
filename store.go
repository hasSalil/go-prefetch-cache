package cache

import "sync"

// ConcurrentKVStore is an interface for a thread-safe key value store that will back the cache
type ConcurrentKVStore interface {
	Set(key, value interface{})
	RemoveIfPredicate(key interface{}, predicate func(value interface{}) bool) bool
	Get(key interface{}) (interface{}, bool)
}

// ConcurrentMapStore is the default in memory implementation of ConcurrentKVStore
type ConcurrentMapStore struct {
	kv   map[interface{}]interface{}
	lock sync.RWMutex
}

func NewConcurrentMapStore() *ConcurrentMapStore {
	return &ConcurrentMapStore{kv: make(map[interface{}]interface{})}
}

func (cm *ConcurrentMapStore) Set(key, value interface{}) {
	cm.lock.Lock()
	cm.kv[key] = value
	cm.lock.Unlock()
}

func (cm *ConcurrentMapStore) RemoveIfPredicate(key interface{}, predicate func(value interface{}) bool) bool {
	cm.lock.Lock()
	defer cm.lock.Unlock()
	v, ok := cm.kv[key]
	if ok && predicate(v) {
		delete(cm.kv, key)
		return true
	}
	return false
}

func (cm *ConcurrentMapStore) Get(key interface{}) (interface{}, bool) {
	cm.lock.RLock()
	v, ok := cm.kv[key]
	cm.lock.RUnlock()
	return v, ok
}
