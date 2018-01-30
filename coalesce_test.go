package cache

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var perCallSleep = time.Millisecond * 7
var totalConcurrency = runtime.NumCPU() * 100

var value = "value"
var errBackend = errors.New("some error")

type mockBackend struct {
	touches uint32
}

func (mb *mockBackend) do() (interface{}, error) {
	time.Sleep(perCallSleep)
	atomic.AddUint32(&mb.touches, 1)
	return value, nil
}

func (mb *mockBackend) doErr() (interface{}, error) {
	time.Sleep(perCallSleep)
	atomic.AddUint32(&mb.touches, 1)
	return nil, errBackend
}

func TestCoalesce(t *testing.T) {
	mb := &mockBackend{}
	doTestCoalesce(t, mb, mb.do, value, nil)
}

func TestCoalesceErr(t *testing.T) {
	mb := &mockBackend{}
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
