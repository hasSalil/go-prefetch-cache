package cache

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"time"
	"unsafe"

	"github.com/OneOfOne/xxhash"
	"github.com/hashicorp/go-multierror"
	"github.com/tildeleb/hashland/jenkins"
)

type ShardedCache struct {
	numShards  int
	caches     []Cache
	keySharder func(key interface{}) (int, error)
}

func NewShardedCache(
	fetcher ItemFetcher,
	globalTimeout *time.Duration,
	timeout ItemTimeout,
	numShards int,
	keyEncoder func(key interface{}) ([]byte, error),
	shardingFunction func(encodedKey []byte) (int, error),
) (Cache, error) {
	if numShards <= 0 {
		return nil, fmt.Errorf("Number of shards must be > 0")
	}
	if keyEncoder == nil {
		keyEncoder = defaultKeyEncoder
	}
	if shardingFunction == nil {
		shardingFunction = defaultShardingFunction(is64Bit(), numShards, rand.Uint32())
	}
	c := &ShardedCache{numShards, make([]Cache, numShards), getKeySharder(keyEncoder, shardingFunction)}
	for i := 0; i < numShards; i++ {
		cache, err := NewCache(fetcher, globalTimeout, timeout)
		if err != nil {
			return nil, fmt.Errorf("Unable to create cache due to %s", err.Error())
		}
		c.caches[i] = cache
	}
	return c, nil
}

func (sc *ShardedCache) WithStore(newStore func() ConcurrentKVStore) Cache {
	for _, cache := range sc.caches {
		cache.WithStore(newStore)
	}
	return sc
}

func (sc *ShardedCache) WithGlobalRefreshInterval(refresh *time.Duration) Cache {
	for _, cache := range sc.caches {
		cache.WithGlobalRefreshInterval(refresh)
	}
	return sc
}

func (sc *ShardedCache) WithGlobalTTL(timeToLive *time.Duration) Cache {
	for _, cache := range sc.caches {
		cache.WithGlobalTTL(timeToLive)
	}
	return sc
}

func (sc *ShardedCache) WithChannelBasedMonitor(newMonitor func() Monitor, hitChSize *int, missChSize *int, refreshChSize *int, setChSize *int, evictChSize *int) Cache {
	for _, cache := range sc.caches {
		cache.WithChannelBasedMonitor(newMonitor, hitChSize, missChSize, refreshChSize, setChSize, evictChSize)
	}
	return sc
}

func (sc *ShardedCache) WithMonitor(newMonitor func() Monitor) Cache {
	for _, cache := range sc.caches {
		cache.WithMonitor(newMonitor)
	}
	return sc
}

func (sc *ShardedCache) Close() error {
	var errs error
	for _, cache := range sc.caches {
		errs = multierror.Append(errs, cache.Close())
	}
	return errs
}

func (sc *ShardedCache) Warmup(concurrency uint, keys []interface{}) error {
	if keys == nil || len(keys) == 0 {
		return fmt.Errorf("Warmup keys cannot be nil or empty")
	}
	keysPerShard := make([][]interface{}, sc.numShards)
	for _, key := range keys {
		s, err := sc.getShard(key)
		if err != nil {
			return fmt.Errorf("Failed to warm up since key %v was not shardable due to %s", key, err)
		}
		keysPerShard[s] = append(keysPerShard[s], key)
	}
	for index, cache := range sc.caches {
		shardKeys := keysPerShard[index]
		if shardKeys != nil && len(shardKeys) > 0 {
			if err := cache.Warmup(concurrency, shardKeys); err != nil {
				return fmt.Errorf("Failed to warm up cache shard %d due to %s", index, err.Error())
			}
		}
	}
	return nil
}

func (sc *ShardedCache) getShard(key interface{}) (int, error) {
	shard, err := sc.keySharder(key)
	if err != nil {
		return -1, fmt.Errorf("Failed to get shard for key %v due to %s", key, err)
	}
	if shard < 0 || shard >= sc.numShards {
		return -1, fmt.Errorf("Sharding function returned out of bounds shard %d for key %v", shard, key)
	}
	return shard, nil
}

func (sc *ShardedCache) Get(key interface{}) (interface{}, error) {
	shard, err := sc.getShard(key)
	if err != nil {
		return nil, fmt.Errorf("Failed to get for key %v due to sharding err: %s", key, err)
	}
	cache := sc.caches[shard]
	if cache == nil {
		return nil, fmt.Errorf("Failed to get for key %v since no cache found at shard %d", key, shard)
	}
	return cache.Get(key)
}

// COULDDO: Always use 32 bit jenkins
func defaultShardingFunction(is64Bit bool, numShards int, seed uint32) func(key []byte) (int, error) {
	if is64Bit {
		return func(key []byte) (int, error) {
			//f := murmur3.New64() //fnv.New64a()
			//f.Write(key)
			//s := f.Sum64() % uint64(numShards)

			//f := jenkins.New() //dgohash.NewMurmur3_x86_32() //.NewJenkins32() //.NewJava32() //.NewSDBM32() //.NewDjb32() //fnv.New64a()
			//f.Write(key)
			//s := f.Sum32() % uint32(numShards)

			s := jenkins.Sum32(key, seed) % uint32(numShards)

			//s := jenkins.Hash264(key, uint64(seed)) % uint64(numShards)

			//s := cityhash.CityHash64WithSeed(key, uint32(len(key)), uint64(seed)) % uint64(numShards)
			// Use Farm64
			//s := farm.Hash64WithSeed(key, uint64(seed)) % uint64(numShards)
			return int(s), nil
		}
	}
	return func(key []byte) (int, error) {
		//Use xxHash32
		s := xxhash.Checksum32S(key, seed) % uint32(numShards)
		return int(s), nil
	}
}

func getKeySharder(
	keyEncoder func(key interface{}) ([]byte, error),
	shardingFunction func(encodedKey []byte) (int, error),
) func(key interface{}) (int, error) {
	return func(key interface{}) (int, error) {
		bytes, err := keyEncoder(key)
		if err != nil {
			return -1, fmt.Errorf("Failed to get bytes from key %v due to %s", key, err)
		}
		s, err := shardingFunction(bytes)
		if err != nil {
			return -1, fmt.Errorf("Failed to shard bytes %v to shard index due to %s", bytes, err.Error())
		}
		return s, nil
	}
}

// defaultKeyEncoder uses gob encoding which works for variable length data but is
// slower than encoding/binary.Write
func defaultKeyEncoder(key interface{}) ([]byte, error) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	if err := enc.Encode(key); err != nil {
		return nil, err
	}
	bytes := b.Bytes()
	if bytes == nil {
		return nil, fmt.Errorf("No bytes in key")
	}
	return bytes, nil
}

func is64Bit() bool {
	x := 42
	a := unsafe.Pointer(&x)
	return unsafe.Sizeof(a) == 8
}
