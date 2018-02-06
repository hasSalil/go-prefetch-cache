package cache

import "time"

// Cache is the interface to a cache implementation that allows providing
// a "thread"-safe store implementation, supports prefetching on a refresh
// interval, TTL for items post put/update, and a hook for monitoring stats
// and metrics
type Cache interface {
	WithStore(newStore func() ConcurrentKVStore) Cache
	WithGlobalRefreshInterval(refresh *time.Duration) Cache
	WithGlobalTTL(timeToLive *time.Duration) Cache
	WithChannelBasedMonitor(newMonitor func() Monitor, hitChSize *int, missChSize *int, refreshChSize *int, setChSize *int, evictChSize *int) Cache
	WithMonitor(newMonitor func() Monitor) Cache
	Get(key interface{}) (interface{}, error)
	Close() error
	Warmup(concurrency uint, keys []interface{}) error
}
