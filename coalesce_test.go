package cache

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var totalConcurrency = runtime.NumCPU() * 100

func TestCoalesce(t *testing.T) {
	mb := &mockBackend{perCallSleep: time.Millisecond * 7}
	doTestCoalesce(t, mb, mb.do, value, nil)
}

func TestCoalesceErr(t *testing.T) {
	mb := &mockBackend{perCallSleep: time.Millisecond * 7}
	doTestCoalesce(t, mb, mb.doErr, nil, errBackend)
}

func doTestCoalesce(t *testing.T, mb *mockBackend, fn func() (interface{}, error), expValue interface{}, expErr error) {
	g := &Group{}
	times := []time.Duration{}
	var timesLock sync.Mutex
	var wg sync.WaitGroup
	wg.Add(totalConcurrency)
	start := time.Now()
	fmt.Println(totalConcurrency)
	for i := 0; i < totalConcurrency; i++ {
		go func() {
			s := time.Now()
			v, err := g.Do("", fn)
			assert.Equal(t, expErr, err)
			assert.Equal(t, expValue, v)
			timesLock.Lock()
			times = append(times, time.Since(s))
			timesLock.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Printf("Time distribution: %v\n", times)
	fmt.Printf("Total time: %v\n", time.Since(start))
	assert.Equal(t, uint32(1), mb.touches)
}
