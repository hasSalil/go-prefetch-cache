package cache

import (
	"errors"
	"sync/atomic"
	"time"
)

var value = "value"
var errBackend = errors.New("some error")

type mockBackend struct {
	perCallSleep time.Duration
	touches      uint32
}

func (mb *mockBackend) do() (interface{}, error) {
	time.Sleep(mb.perCallSleep)
	atomic.AddUint32(&mb.touches, 1)
	return value, nil
}

func (mb *mockBackend) doErr() (interface{}, error) {
	time.Sleep(mb.perCallSleep)
	atomic.AddUint32(&mb.touches, 1)
	return nil, errBackend
}

type mockFetcher struct {
	*mockBackend
	cancelledReqs map[interface{}]struct{}
}

func (mf *mockFetcher) Close() error {
	return nil
}

func (mf *mockFetcher) FetchItem(key interface{}) (*ItemFetchResponse, error) {
	ks := key.(string)
	fn := mf.do
	if ks == keyWithError {
		fn = mf.doErr
	}
	r, err := fn()
	if err != nil {
		return nil, err
	}
	return &ItemFetchResponse{Value: r}, nil
}

func (mf *mockFetcher) Cancel(key interface{}) {
	if mf.cancelledReqs == nil {
		mf.cancelledReqs = make(map[interface{}]struct{})
	}
	mf.cancelledReqs[key] = struct{}{}
}
