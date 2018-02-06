package cache

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/montanaflynn/stats"
	"github.com/stretchr/testify/assert"
)

var monChSize = 20000000
var totalRequestPerRound = 1000
var numKeys = 1
var keyContention = totalRequestPerRound / numKeys
var rounds = 1000
var cacheTestBackendDelay = time.Millisecond * 2

func getKeys() []interface{} {
	ks := []interface{}{}
	for k := 0; k < numKeys; k++ {
		ks = append(ks, strconv.Itoa(k))
	}
	return ks
}

type testMonitor struct {
	lock      sync.Mutex
	latencies []stats.Float64Data
	counts    []uint32
	errCounts []uint32
}

func newTestMonitor() Monitor {
	return &testMonitor{
		latencies: make([]stats.Float64Data, 4, 4),
		counts:    make([]uint32, 5, 5),
		errCounts: make([]uint32, 2, 2),
	}
}

func (tm *testMonitor) Hit(latency time.Duration) {
	tm.lock.Lock()
	if tm.latencies[0] == nil {
		tm.latencies[0] = make([]float64, 0, totalRequestPerRound*rounds)
	}
	tm.latencies[0] = append(tm.latencies[0], float64(latency.Nanoseconds()))
	tm.lock.Unlock()
	atomic.AddUint32(&(tm.counts[0]), 1)
}

func (tm *testMonitor) Miss(latency time.Duration, err error) {
	tm.lock.Lock()
	if tm.latencies[1] == nil {
		tm.latencies[1] = make([]float64, 0, totalRequestPerRound*rounds)
	}
	tm.latencies[1] = append(tm.latencies[1], float64(latency.Nanoseconds()))
	tm.lock.Unlock()
	atomic.AddUint32(&(tm.counts[1]), 1)
	if err != nil {
		atomic.AddUint32(&(tm.errCounts[0]), 1)
	}
}

func (tm *testMonitor) Refresh(latency time.Duration, err error) {
	tm.lock.Lock()
	if tm.latencies[2] == nil {
		tm.latencies[2] = make([]float64, 0, totalRequestPerRound*rounds)
	}
	tm.latencies[2] = append(tm.latencies[2], float64(latency.Nanoseconds()))
	tm.lock.Unlock()
	atomic.AddUint32(&(tm.counts[2]), 1)
	if err != nil {
		atomic.AddUint32(&(tm.errCounts[1]), 1)
	}

}

func (tm *testMonitor) getRefreshCount() uint32 {
	return atomic.LoadUint32(&(tm.counts[2]))
}

func (tm *testMonitor) Set(latency time.Duration) {
	tm.lock.Lock()
	if tm.latencies[3] == nil {
		tm.latencies[3] = make([]float64, 0, totalRequestPerRound*rounds)
	}
	tm.latencies[3] = append(tm.latencies[3], float64(latency.Nanoseconds()))
	tm.lock.Unlock()
	atomic.AddUint32(&(tm.counts[3]), 1)
}

func (tm *testMonitor) getSetCount() uint32 {
	return atomic.LoadUint32(&(tm.counts[3]))
}

func (tm *testMonitor) Evict() {
	atomic.AddUint32(&(tm.counts[4]), 1)
}

func (tm *testMonitor) Close() error {
	return nil
}

func (tm *testMonitor) summary() string {
	tm.lock.Lock()
	defer tm.lock.Unlock()
	var buffer bytes.Buffer
	buffer.WriteString("CACHE SUMMARY STATS:\nHITS:\n")
	tm.summarizeLatencies(&buffer, 0)
	hits := tm.counts[0]
	buffer.WriteString(fmt.Sprintf("\tHit count: %d\n", hits))

	buffer.WriteString("\nMISSES:\n")
	tm.summarizeLatencies(&buffer, 1)
	misses := tm.counts[1]
	buffer.WriteString(fmt.Sprintf("\tMiss count: %d\n", misses))
	buffer.WriteString(fmt.Sprintf("\tMiss errors: %d\n", tm.errCounts[0]))
	buffer.WriteString(fmt.Sprintf("\tHit ratio: %f\n", 100.0*float64(hits)/float64(hits+misses)))

	buffer.WriteString("\nREFRESHES:\n")
	tm.summarizeLatencies(&buffer, 2)
	refs := tm.counts[2]
	buffer.WriteString(fmt.Sprintf("\tRefresh count: %d\n", refs))
	buffer.WriteString(fmt.Sprintf("\tRefresh errors: %d\n", tm.errCounts[1]))

	buffer.WriteString("\nSETS:\n")
	tm.summarizeLatencies(&buffer, 3)
	sets := tm.counts[3]
	buffer.WriteString(fmt.Sprintf("\tSet count: %d\n", sets))

	buffer.WriteString("\nEVICTS:\n")
	evicts := tm.counts[4]
	buffer.WriteString(fmt.Sprintf("\tEvict count: %d\n", evicts))

	return buffer.String()
}

func (tm *testMonitor) summarizeLatencies(buffer *bytes.Buffer, i int) {
	samples := len(tm.latencies[i])
	if samples > 0 {
		sort.Float64s(tm.latencies[i])
		buffer.WriteString("\tp0\tp25\tp50\tp75\tp95\tp99\tp999\tp9999\tp99999\tp999999\tp9999999\tp99999999\t|\tpmax3\tpmax2\tpmax1\tp100\tavg\n")
		min, _ := stats.Min(tm.latencies[i])
		p25, _ := stats.PercentileNearestRank(tm.latencies[i], 25)
		med, _ := stats.PercentileNearestRank(tm.latencies[i], 50)
		p75, _ := stats.PercentileNearestRank(tm.latencies[i], 75)
		p95, _ := stats.PercentileNearestRank(tm.latencies[i], 95)
		p99, _ := stats.PercentileNearestRank(tm.latencies[i], 99)
		p999, _ := stats.PercentileNearestRank(tm.latencies[i], 99.9)
		p9999, _ := stats.PercentileNearestRank(tm.latencies[i], 99.99)
		p99999, _ := stats.PercentileNearestRank(tm.latencies[i], 99.999)
		p999999, _ := stats.PercentileNearestRank(tm.latencies[i], 99.9999)
		p9999999, _ := stats.PercentileNearestRank(tm.latencies[i], 99.99999)
		p99999999, _ := stats.PercentileNearestRank(tm.latencies[i], 99.999999)
		max3 := -1.0
		if samples >= 4 {
			max3 = tm.latencies[i][samples-4]
		}
		max2 := -1.0
		if samples >= 3 {
			max2 = tm.latencies[i][samples-3]
		}
		max1 := -1.0
		if samples >= 2 {
			max1 = tm.latencies[i][samples-2]
		}
		max, _ := stats.Max(tm.latencies[i])
		avg, _ := stats.Mean(tm.latencies[i])

		buffer.WriteString(fmt.Sprintf(
			"\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t%f\t|\t%f\t%f\t%f\t%f\t%f\n",
			min, p25, med, p75, p95, p99, p999, p9999, p99999, p999999, p9999999, p99999999, max3, max2, max1, max, avg,
		))
	}
}

func doCacheTest(pc *PrefetchCache, t *testing.T) (map[interface{}]int, map[string]int) {
	start := time.Now()
	pc = (pc.WithChannelBasedMonitor(
		newTestMonitor, &monChSize, &monChSize, &monChSize, &monChSize, &monChSize,
	)).(*PrefetchCache)
	pc.debugEvents = make(chan debugEvent, 20000)
	ks := getKeys()
	vals := make(chan interface{}, totalRequestPerRound*rounds)
	for r := 0; r < rounds; r++ {
		var wg sync.WaitGroup
		wg.Add(len(ks) * keyContention)
		for _, k := range ks {
			for i := 0; i < keyContention; i++ {
				go func(key interface{}) {
					v, err := pc.Get(key)
					if err != nil {
						t.Fatal(err)
					}
					vals <- v
					wg.Done()
				}(k)
			}
		}
		wg.Wait()
	}
	pc.Close()
	close(vals)
	close(pc.debugEvents)
	uniqVals := make(map[interface{}]int)
	for val := range vals {
		if _, ok := uniqVals[val]; ok {
			uniqVals[val]++
		} else {
			uniqVals[val] = 1
		}
	}

	debEvents := make(map[string]int)
	for evt := range pc.debugEvents {
		if _, ok := debEvents[evt.name]; ok {
			debEvents[evt.name]++
		} else {
			debEvents[evt.name] = 1
		}
	}
	t.Logf("Test duration: %v", time.Since(start))
	t.Logf("Unique get vals: %d", len(uniqVals))
	m := pc.monitor.(*ChannelBasedMonitor)
	t.Log(m.monitor.(*testMonitor).summary())

	return uniqVals, debEvents
}

func TestWarmedNoRefreshNoEvict(t *testing.T) {
	c, err := NewCache(
		&mockFetcher{mockBackend: &mockBackend{useTimestampValue: true, perCallSleep: cacheTestBackendDelay}},
		nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	pc := c.(*PrefetchCache)
	keys := getKeys()
	if err := c.Warmup(10, keys); err != nil {
		t.Fatal(err)
	}
	uniqVals, _ := doCacheTest(pc, t)
	assert.Equal(t, len(keys), len(uniqVals))
	mon := ((pc.monitor.(*ChannelBasedMonitor)).monitor).(*testMonitor)
	assert.Equal(t, uint32(0), mon.getRefreshCount())
}

func TestWarmedRefreshNoEvict(t *testing.T) {
	refresh := time.Second * 2
	c, err := NewCache(
		&mockFetcher{mockBackend: &mockBackend{useTimestampValue: true, perCallSleep: cacheTestBackendDelay}},
		nil, nil,
	)
	pc := c.(*PrefetchCache)
	if err != nil {
		t.Fatal(err)
	}
	pc = pc.WithGlobalRefreshInterval(&refresh).(*PrefetchCache)
	keys := getKeys()
	if err := pc.Warmup(10, keys); err != nil {
		t.Fatal(err)
	}
	doCacheTest(pc, t)
	mon := ((pc.monitor.(*ChannelBasedMonitor)).monitor).(*testMonitor)
	refreshes := int(mon.getRefreshCount())
	assert.True(t, refreshes > 0)
}

func TestColdRefreshNoEvict(t *testing.T) {
	refresh := time.Millisecond * 10
	c, err := NewCache(
		&mockFetcher{mockBackend: &mockBackend{useTimestampValue: true, perCallSleep: cacheTestBackendDelay}},
		nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	pc := c.(*PrefetchCache)
	pc = pc.WithGlobalRefreshInterval(&refresh).(*PrefetchCache)
	uniqVals, debEvents := doCacheTest(pc, t)
	spew.Dump(uniqVals)
	spew.Dump(debEvents)
	mon := ((pc.monitor.(*ChannelBasedMonitor)).monitor).(*testMonitor)
	refreshes := int(mon.getRefreshCount())
	assert.True(t, refreshes > 0)
	assert.Equal(t, int(mon.getSetCount()), len(uniqVals))
}

//TODO:
// Test slow/failed fetches causing repeated misses
// Test ttl eviction
