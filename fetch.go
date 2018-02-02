package cache

import (
	"context"
	"errors"
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
	Close() error
}

type ItemTimeout func(key interface{}) *time.Duration

// TODO: Rename to refreshHandler
type fetchManager struct {
	c             *Cache
	fetcher       ItemFetcher
	globalTimout  *time.Duration
	timeout       ItemTimeout
	coalesceGroup *Group
}

func newFetchManager(c *Cache, fetch ItemFetcher, globalTimout *time.Duration, timeout ItemTimeout) *fetchManager {
	return &fetchManager{c: c, fetcher: fetch, timeout: timeout, globalTimout: globalTimout, coalesceGroup: &Group{}}
}

func (fm *fetchManager) fetch(key interface{}, nonce string) (*ItemFetchResponse, error) {
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
		coalesceKey := struct {
			key   interface{}
			nonce string
		}{key: key, nonce: nonce}
		resp, err := fm.coalesceGroup.Do(coalesceKey, func() (interface{}, error) {
			fetched, err := fm.fetcher.FetchItem(key)
			if fm.c != nil {
				// defensive against both fetched and err being nil
				// if both non nil then fetched is ignored, and err is set
				var r *time.Duration
				var t *time.Duration
				var val interface{}
				if err == nil {
					if fetched != nil {
						r = fetched.RefreshInterval
						t = fetched.TimeToLive
						val = fetched.Value
					} else {
						err = errors.New("no response from backend")
					}
				}

				// COULDDO: If miss latencies are too high, we can put the result on
				// a channel and return or do set in a go routine. That may result
				// in extra FetchItem calls to backend though.
				fm.c.set(key, r, t, val, nonce, err)
			}
			return fetched, err
		})
		var r *ItemFetchResponse
		if resp != nil {
			r = resp.(*ItemFetchResponse)
		}
		pack := struct {
			r   *ItemFetchResponse
			err error
		}{r, err}
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

func (fm *fetchManager) close() error {
	return fm.fetcher.Close()
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
