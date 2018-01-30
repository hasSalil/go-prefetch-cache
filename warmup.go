package cache

import (
	"math"
	"sync"

	multierror "github.com/hashicorp/go-multierror"
)

type CacheWarmer interface {
	WarmUp(c *Cache) error
}

func (c *Cache) Warmup(concurrency uint, keys ...interface{}) error {
	if keys == nil || len(keys) == 0 {
		return nil
	}
	con := int(concurrency)
	numItems := len(keys)
	batchMaxLen := int(math.Ceil(float64(numItems) / float64(con)))
	if con > numItems {
		con = numItems
		batchMaxLen = 1
	}
	var wg sync.WaitGroup
	errCh := make(chan error, numItems)
	wg.Add(con)
	for i := 0; i < con; i++ {
		s := i * batchMaxLen
		e := (i + 1) * batchMaxLen
		if e > numItems {
			e = numItems
		}
		keyShard := keys[s:e]
		go func() {
			for _, key := range keyShard {
				if _, err := c.RefreshKey(key); err != nil {
					errCh <- err
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	close(errCh)
	var errs error
	for fetchErr := range errCh {
		errs = multierror.Append(errs, fetchErr)
	}
	return errs
}

func (c *Cache) WarmupChannel(keyCh <-chan *interface{}) error {
	var errs error
	for key := range keyCh {
		if _, err := c.RefreshKey(key); err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	return errs
}
