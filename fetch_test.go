package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const keyUsingGlobalTimeout = "global"
const keyUsingOverrideTimeout = "override"
const keyWithError = "errKey"

type mockFetcherWithTimeout struct {
	*mockBackend
	cancelledReqs map[interface{}]struct{}
}

func (mf *mockFetcherWithTimeout) FetchItem(key interface{}) (*ItemFetchResponse, error) {
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

func (mf *mockFetcherWithTimeout) Cancel(key interface{}) {
	if mf.cancelledReqs == nil {
		mf.cancelledReqs = make(map[interface{}]struct{})
	}
	mf.cancelledReqs[key] = struct{}{}
}

func TestResolveTimeout(t *testing.T) {
	gd := time.Second
	fm := &fetchManager{globalTimout: &gd, timeout: func(key interface{}) *time.Duration {
		ks := key.(string)
		if ks == keyUsingOverrideTimeout {
			d := (time.Second * 2)
			return &d
		}
		return nil
	}}
	assert.Equal(t, time.Second, *(fm.resolveTimeout(keyUsingGlobalTimeout)))
	assert.Equal(t, time.Second*2, *(fm.resolveTimeout(keyUsingOverrideTimeout)))

	fm.globalTimout = nil
	assert.Nil(t, fm.resolveTimeout(keyUsingGlobalTimeout))
	assert.Equal(t, time.Second*2, *(fm.resolveTimeout(keyUsingOverrideTimeout)))
}

func TestFetchTimeout(t *testing.T) {
	gd := time.Second * 2
	fm := &fetchManager{
		coalesceGroup: &Group{},
		fetcher:       &mockFetcherWithTimeout{mockBackend: &mockBackend{}},
		globalTimout:  &gd,
		timeout: func(key interface{}) *time.Duration {
			ks := key.(string)
			if ks == keyUsingOverrideTimeout {
				d := time.Millisecond
				return &d
			}
			return nil
		},
	}

	r, err := fm.fetch(keyUsingGlobalTimeout)
	assert.NotNil(t, r)
	assert.Equal(t, value, r.Value)
	assert.Nil(t, err)

	r, err = fm.fetch(keyUsingOverrideTimeout)
	assert.Nil(t, r)
	assert.NotNil(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)

	r, err = fm.fetch(keyWithError)
	assert.Nil(t, r)
	assert.NotNil(t, err)
	assert.Equal(t, errBackend, err)
}

func TestFetchNoTimeouts(t *testing.T) {
	fm := &fetchManager{
		coalesceGroup: &Group{},
		fetcher:       &mockFetcherWithTimeout{mockBackend: &mockBackend{}},
	}

	r, err := fm.fetch("some key")
	assert.NotNil(t, r)
	assert.Equal(t, value, r.Value)
	assert.Nil(t, err)

	r, err = fm.fetch(keyWithError)
	assert.Nil(t, r)
	assert.NotNil(t, err)
	assert.Equal(t, errBackend, err)
}
