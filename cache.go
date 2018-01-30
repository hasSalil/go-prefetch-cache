package cache

import (
	"fmt"
	"time"
)

// Cache manages a key:value store, refreshing (fetching fresh) items using the ItemFetcher
// and evicting stale items per global or key level control parameters.
//
// Refreshes occur in the background by prefetching items as per the refresh schedule.
// Eviction also occurs in the background per the eviction schedule.
// In the event of a cache miss, the item is fetched from backend synchronously using the ItemFetcher.
//
// This cache interacts with the underlying store via its methods, and can guarantee freedom from
// deadlock only if these are "thread"-safe (default in memory store is "thread"-safe), and fetch
// timeouts are used/fetch function times itself out.
//
// It is also agnostic to other processes that manipulate the store such as LRU, LFU eviction mechanisms.
// Ensuring that these processes working alongside data refresh and eviction of this cache in a sensible
// manner is the responsibility of the developer.
type Cache struct {
	store        ConcurrentKVStore
	fetchManager *fetchManager
	refreshErrCh chan error
	evictErrCh   chan error

	refreshInterval *time.Duration
	timeToLive      *time.Duration
}

// NewCache creates a new prefetch cache using the specified ItemFetcher and fetch timeout
func NewCache(fetch ItemFetch, globalTimeout *time.Duration, timeout ItemTimeout) (*Cache, error) {
	if fetch == nil {
		return nil, fmt.Errorf("Item fetch function cannot be nil")
	}
	if timeout == nil {
		timeout = func(key interface{}) *time.Duration {
			// never timeout
			return nil
		}
	}
	return &Cache{
		store:        NewConcurrentMapStore(),
		fetchManager: newFetchManager(fetch, globalTimeout, timeout),
		refreshErrCh: make(chan error, 100),
		evictErrCh:   make(chan error, 100),
	}, nil
}

func (c *Cache) WithStore(store ConcurrentKVStore) *Cache {
	c.store = store
	return c
}

func (c *Cache) WithGlobalCacheControls(refresh *time.Duration, timeToLive *time.Duration) *Cache {
	c.refreshInterval = refresh
	c.timeToLive = timeToLive
	return c
}

func (c *Cache) Get(key interface{}) (interface{}, error) {
	v, ok := c.store.Get(key)
	if ok {
		// hit
		return v, nil
	}
	// miss
	return c.RefreshKey(key)
}

// RefreshKey fetches item for key, places it into cache and returns the value
func (c *Cache) RefreshKey(key interface{}) (interface{}, error) {
	resp, err := c.fetchManager.fetch(key)
	if err != nil {
		return nil, fmt.Errorf("Warm up fetch failed for %v due to %s", key, err)
	}
	if err := c.Set(key, resp.RefreshInterval, resp.TimeToLive, resp.Value); err != nil {
		return nil, err
	}
	return resp.Value, nil
}

func (c *Cache) Set(key interface{}, refreshInterval, timeToLive *time.Duration, value interface{}) error {
	r, ttl := c.resolveControls(refreshInterval, timeToLive)
	generation := time.Now().UnixNano()
	c.store.Set(key, Item{Value: value, Generation: generation})

	if r != nil {
		// Schedule next refresh
		time.AfterFunc(*r, func() {
			if _, err := c.RefreshKey(key); err != nil {
				c.refreshErrCh <- err
			}
		})
	}

	if ttl != nil {
		// Schedule eviction that evicts if previous or current generation
		time.AfterFunc(*ttl, func() {
			c.store.RemoveIfPredicate(key, func(value interface{}) bool {
				item, ok := value.(Item)
				if !ok {
					// yet another reason why generics are needed
					c.evictErrCh <- fmt.Errorf("Stored value for key was not of type Item: %v", value)
					return false
				}
				return item.Generation <= generation
			})
		})
	}
	return nil
}

func (c *Cache) resolveControls(refreshInterval, timeToLive *time.Duration) (*time.Duration, *time.Duration) {
	r := c.refreshInterval
	ttl := c.timeToLive
	if refreshInterval != nil {
		r = refreshInterval
	}
	if timeToLive != nil {
		ttl = timeToLive
	}
	return r, ttl
}
