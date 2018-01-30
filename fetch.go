package cache

import (
	"time"
)

type ItemFetchResponse struct {
	Value           interface{}
	RefreshInterval *time.Duration
	TimeToLive      *time.Duration
}

type ItemFetch func(key interface{}) (*ItemFetchResponse, error)

type ItemTimeout func(key interface{}) *time.Duration

type fetchManager struct {
	fetchFn       ItemFetch
	globalTimout  *time.Duration
	timeout       ItemTimeout
	coalesceGroup *Group
}

func newFetchManager(fetch ItemFetch, globalTimout *time.Duration, timeout ItemTimeout) *fetchManager {
	return &fetchManager{fetchFn: fetch, timeout: timeout, globalTimout: globalTimout, coalesceGroup: &Group{}}
}

func (fm *fetchManager) fetch(key interface{}) (*ItemFetchResponse, error) {

	// TODO: resolve timeout and implement timeout
	resp, err := fm.coalesceGroup.Do(key, func() (interface{}, error) {
		return fm.fetchFn(key)
	})
	if err != nil {
		return nil, err
	}
	return resp.(*ItemFetchResponse), nil
}

func (fm *fetchManager) resolveTimeout(key interface{}) *time.Duration {
	timeout := fm.globalTimout
	t := fm.timeout(key)
	if t != nil {
		timeout = t
	}
	return timeout
}
