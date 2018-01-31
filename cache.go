package cache

import (
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
)

var errClosed = fmt.Errorf("Operation not permitted after closing cache")

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
	store           ConcurrentKVStore
	fetchManager    *fetchManager
	refreshInterval *time.Duration
	timeToLive      *time.Duration
	monitor         Monitor
	closed          bool
	lock            sync.RWMutex
}

// NewCache creates a new prefetch cache using the provided ItemFetcher, global timeout,
// and timeout override function
// ItemFetcher cannot be nil
func NewCache(fetcher ItemFetcher, globalTimeout *time.Duration, timeout ItemTimeout) (*Cache, error) {
	if fetcher == nil {
		return nil, fmt.Errorf("Item fetcher cannot be nil")
	}
	if timeout == nil {
		timeout = func(key interface{}) *time.Duration {
			// defaults to no per key timeout overrides
			return nil
		}
	}
	return &Cache{
		store:        NewConcurrentMapStore(),
		fetchManager: newFetchManager(fetcher, globalTimeout, timeout),
	}, nil
}

// WithStore builds a cache with the provided store implementation
func (c *Cache) WithStore(store ConcurrentKVStore) *Cache {
	c.store = store
	return c
}

// WithGlobalRefreshInterval builds a cache with the provided refresh interval
// At every refresh interval, keys present in the cache are updated with
// fresh values from the ItemFetcher.
//
// Refresh intervals can be overriden for a particular key if some refresh interval
// is returned for that key by the ItemFetcher.
//
// If there is no global refresh and the ItemFetcher does not return a refresh interval
// for a key, then that key will not be refreshed.
func (c *Cache) WithGlobalRefreshInterval(refresh *time.Duration) *Cache {
	c.refreshInterval = refresh
	return c
}

// WithGlobalTTL builds a cache with the provided TTL
// At every ttl interval, keys present in the cache are evicted if for some
// reason the key was not successfully refreshed after the last Set operation
// against this key (this can happen if the refresh failed to fetch the item, OR if
// auto refresh is not enabled for the item, and no Set was called against the key
// before TTL expired).
//
// TTL intervals can be overriden for a particular key if some ttl interval
// is returned for that key by the ItemFetcher.
//
// If there is no global ttl and the ItemFetcher does not return a ttl interval
// for a key, then that key will not be evicted.
func (c *Cache) WithGlobalTTL(timeToLive *time.Duration) *Cache {
	c.timeToLive = timeToLive
	return c
}

// WithChannelBasedMonitor builds a cache with a non blocking channel based
// default monitor that wraps the provided (potentially) blocking monitor
func (c *Cache) WithChannelBasedMonitor(
	monitor Monitor,
	hitChSize *int,
	missChSize *int,
	refreshChSize *int,
	setChSize *int,
	evictChSize *int,
) *Cache {
	c.monitor = NewChannelBasedMonitor(
		monitor, hitChSize, missChSize, refreshChSize, setChSize, evictChSize,
	)
	return c
}

// WithMonitor builds a cache with the provided monitor
func (c *Cache) WithMonitor(monitor Monitor) *Cache {
	c.monitor = monitor
	return c
}

// Get retrieves item from cache if present, else fetches item from backend,
// places it into the cache and returns the value or error
func (c *Cache) Get(key interface{}) (interface{}, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.closed {
		return nil, errClosed
	}
	s := time.Now()
	hit, v, err := c.get(key)
	if c.monitor != nil {
		latency := time.Since(s)
		if hit {
			c.monitor.Hit(latency)
		} else {
			c.monitor.Miss(latency, err)
		}
	}
	return v, err
}

func (c *Cache) get(key interface{}) (bool, interface{}, error) {
	v, ok := c.store.Get(key)
	if ok {
		// hit
		return true, v, nil
	}
	// miss
	v, err := c.RefreshKey(key)
	return false, v, err
}

// RefreshKey fetches item for key, places it into the cache and returns the value or error
func (c *Cache) RefreshKey(key interface{}) (interface{}, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.closed {
		return nil, errClosed
	}
	s := time.Now()
	v, err := c.refreshKey(key)
	if c.monitor != nil {
		latency := time.Since(s)
		c.monitor.Refresh(latency, err)
	}
	return v, err
}

func (c *Cache) refreshKey(key interface{}) (interface{}, error) {
	resp, err := c.fetchManager.fetch(key)
	if err != nil {
		return nil, fmt.Errorf("Warm up fetch failed for %v due to %s", key, err)
	}
	c.set(key, resp.RefreshInterval, resp.TimeToLive, resp.Value)
	return resp.Value, nil
}

// Set places the key value pair into the cache along with refresh and time to live metadata
func (c *Cache) Set(key interface{}, refreshInterval, timeToLive *time.Duration, value interface{}) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if c.closed {
		return errClosed
	}
	c.set(key, refreshInterval, timeToLive, value)
	return nil
}

func (c *Cache) set(key interface{}, refreshInterval, timeToLive *time.Duration, value interface{}) {
	s := time.Now()
	r, ttl := c.resolveControls(refreshInterval, timeToLive)
	generation := time.Now().UnixNano()
	c.store.Set(key, item{value: value, generation: generation})

	if r != nil {
		// Schedule next refresh
		time.AfterFunc(*r, func() {
			c.RefreshKey(key)
		})
	}

	if ttl != nil {
		// Schedule eviction that evicts if previous or current generation
		time.AfterFunc(*ttl, func() {
			if removed := c.store.RemoveIfPredicate(key, func(i interface{}) bool {
				item, _ := i.(item)
				return item.generation <= generation
			}); removed {
				if c.monitor != nil {
					c.monitor.Evict()
				}
			}
		})
	}
	if c.monitor != nil {
		latency := time.Since(s)
		c.monitor.Set(latency)
	}
}

func (c *Cache) Close() error {
	c.lock.Lock()
	c.closed = true
	c.lock.Unlock()
	var errs error
	if c.monitor != nil {
		if err := c.monitor.Close(); err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	if err := c.fetchManager.close(); err != nil {
		errs = multierror.Append(errs, err)
	}
	return errs
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
