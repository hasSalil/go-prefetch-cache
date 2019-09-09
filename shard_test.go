package cache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/stretchr/testify/assert"
)

var keyCount = 100000
var testShardCount = 8192
var seed = uint32(100)

func TestShardDistributionSeqInt64(t *testing.T) {
	seq := make([]interface{}, keyCount)
	for i := int64(0); i < int64(keyCount); i++ {
		seq[i] = int64(i)
	}
	testDistributionAllArchs(t, seq...)
}

func TestShardDistributionSeq3Int64(t *testing.T) {
	seq := make([]interface{}, keyCount)
	for i := int64(0); i < int64(keyCount); i++ {
		v := int64(i)
		seq[i] = struct {
			A int64
			B int64
			C int64
		}{A: v, B: v, C: v}
	}
	testDistributionAllArchs(t, seq...)
}

func TestShardDistributionRandom3Int64(t *testing.T) {
	ran := make([]interface{}, keyCount)
	for i := uint64(0); i < uint64(keyCount); i++ {
		v := rand.Uint64()
		ran[i] = struct {
			A uint64
			B uint64
			C uint64
		}{A: v, B: v, C: v}
	}
	testDistributionAllArchs(t, ran...)
}

func testDistributionAllArchs(t *testing.T, keys ...interface{}) {
	doDistributionTest(t, "64bit", true, keys...)
	doDistributionTest(t, "32bit", false, keys...)
}

func doDistributionTest(t *testing.T, arch string, is64 bool, keys ...interface{}) {
	f := defaultShardingFunction(is64, testShardCount, seed)

	distribution := make([]float64, testShardCount)
	getShard := getKeySharder(binEncoder, f)
	for _, key := range keys {
		s, err := getShard(key)
		if err != nil {
			t.Fatal(err)
		}
		assert.True(t, s >= 0 && s < testShardCount, "shard index is %d", s)
		distribution[s]++
	}

	s := time.Now()
	for _, key := range keys {
		getShard(key)
	}
	duration := time.Since(s)
	stddev, err := stats.StandardDeviationSample(distribution)
	if err != nil {
		t.Fatal(err)
	}
	iqr, err := stats.InterQuartileRange(distribution)
	if err != nil {
		t.Fatal(err)
	}
	quarts, err := stats.Quartile(distribution)
	if err != nil {
		t.Fatal(err)
	}
	min, err := stats.Min(distribution)
	if err != nil {
		t.Fatal(err)
	}
	max, err := stats.Max(distribution)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf(
		"%s expected value=%f, min=%f, q1=%f, q2=%f, q3=%f, max=%f, stddev=%f, iqr=%f, per key duration ns: %d",
		arch,
		float64(keyCount)/float64(testShardCount),
		min,
		quarts.Q1,
		quarts.Q2,
		quarts.Q3,
		max,
		stddev,
		iqr,
		duration.Nanoseconds()/int64(len(keys)),
	)
}

type mockShardedFetcher struct {
	*mockFetcher
	lock             sync.Mutex
	requestedFetches map[interface{}]int
}

func (sf *mockShardedFetcher) FetchItem(key interface{}) (*ItemFetchResponse, error) {
	sf.lock.Lock()
	if sf.requestedFetches == nil {
		sf.requestedFetches = make(map[interface{}]int)
	}

	sf.requestedFetches[key]++
	sf.lock.Unlock()
	return sf.mockFetcher.FetchItem(key)
}

func TestWarmupNilOrEmpty(t *testing.T) {
	c, err := getShardedCache(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	sc := c.(*ShardedCache)
	err = sc.Warmup(10, nil)
	assert.NotNil(t, err)
	keys := make([]interface{}, 0)
	err = sc.Warmup(10, keys)
	assert.NotNil(t, err)
}

func TestWarmupSparseKeys(t *testing.T) {
	fetcher := getMockFetcher()
	c, err := getShardedCache(128, fetcher)
	if err != nil {
		t.Fatal(err)
	}
	sc := c.(*ShardedCache)
	keys := []interface{}{"1", "2"}
	err = sc.Warmup(10, keys)
	assert.Nil(t, err)
	for _, k := range keys {
		assert.Equal(t, 1, fetcher.requestedFetches[k])
		v, err := sc.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, value, v)
	}
}

func TestWarmupDenseKeys(t *testing.T) {
	fetcher := getMockFetcher()
	c, err := getShardedCache(2, fetcher)
	if err != nil {
		t.Fatal(err)
	}
	sc := c.(*ShardedCache)
	keys := []interface{}{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	err = sc.Warmup(10, keys)
	assert.Nil(t, err)
	for _, k := range keys {
		assert.Equal(t, 1, fetcher.requestedFetches[k])
		v, err := sc.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, value, v)
	}
}

func binEncoder(key interface{}) ([]byte, error) {
	var b bytes.Buffer
	if err := binary.Write(&b, binary.LittleEndian, key); err != nil {
		return nil, err
	}
	bytes := b.Bytes()
	if bytes == nil {
		return nil, fmt.Errorf("No bytes in key")
	}
	return bytes, nil
}

func getShardedCache(numShards int, fetcher *mockShardedFetcher) (Cache, error) {
	return NewShardedCache(fetcher, nil, nil, numShards, nil, nil)
}

func getMockFetcher() *mockShardedFetcher {
	return &mockShardedFetcher{mockFetcher: &mockFetcher{mockBackend: &mockBackend{}}}
}
