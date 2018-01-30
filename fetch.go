package cache

import (
	"context"
	"time"
)

type ItemFetchResponse struct {
	Value           interface{}
	RefreshInterval *time.Duration
	TimeToLive      *time.Duration
}

type ItemFetcher interface {
	FetchItem(key interface{}) (*ItemFetchResponse, error)
	Cancel(key interface{})
}

type ItemTimeout func(key interface{}) *time.Duration

type fetchManager struct {
	fetcher       ItemFetcher
	globalTimout  *time.Duration
	timeout       ItemTimeout
	coalesceGroup *Group
}

func newFetchManager(fetch ItemFetcher, globalTimout *time.Duration, timeout ItemTimeout) *fetchManager {
	return &fetchManager{fetcher: fetch, timeout: timeout, globalTimout: globalTimout, coalesceGroup: &Group{}}
}

func (fm *fetchManager) fetch(key interface{}) (*ItemFetchResponse, error) {
	t := fm.resolveTimeout(key)
	ctx, cancel := context.WithCancel(context.Background())
	if t != nil {
		ctx, cancel = context.WithTimeout(context.Background(), *t)
	}
	defer cancel()

	c := make(chan struct {
		r   *ItemFetchResponse
		err error
	}, 1)
	go func() {
		resp, err := fm.coalesceGroup.Do(key, func() (interface{}, error) {
			return fm.fetcher.FetchItem(key)
		})
		pack := struct {
			r   *ItemFetchResponse
			err error
		}{resp.(*ItemFetchResponse), err}
		c <- pack
	}()
	select {
	case <-ctx.Done():
		fm.fetcher.Cancel(key)
		return nil, ctx.Err()
	case ok := <-c:
		return ok.r, ok.err
	}
}

func (fm *fetchManager) resolveTimeout(key interface{}) *time.Duration {
	timeout := fm.globalTimout
	if fm.timeout != nil {
		t := fm.timeout(key)
		if t != nil {
			timeout = t
		}
	}
	return timeout
}
