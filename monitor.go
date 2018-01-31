package cache

import (
	"sync"
	"time"
)

// Monitor is the interface for monitoring stats and errors for operations.
// Stats callbacks block the corresponding operations; it is the responsibility
// of the code using the cache to ensure that the implementation of the hooked in
// monitor does not deadlock, or performance bottleneck these operations.
type Monitor interface {
	Hit(latency time.Duration)
	Miss(latency time.Duration, err error)
	Refresh(latency time.Duration, err error)
	Set(latency time.Duration)
	Evict()
	Close() error
}

type event struct {
	latency time.Duration
	err     error
}

const DefaultMonitorChannelSize = 100

type ChannelBasedMonitor struct {
	monitor   Monitor
	hitCh     chan event
	missCh    chan event
	refreshCh chan event
	setCh     chan event
	evictCh   chan event
	wg        sync.WaitGroup
}

func NewChannelBasedMonitor(
	monitor Monitor,
	hitChSize *int,
	missChSize *int,
	refreshChSize *int,
	setChSize *int,
	evictChSize *int,
) *ChannelBasedMonitor {
	m := &ChannelBasedMonitor{
		monitor:   monitor,
		hitCh:     make(chan event, getSize(hitChSize)),
		missCh:    make(chan event, getSize(missChSize)),
		refreshCh: make(chan event, getSize(refreshChSize)),
		setCh:     make(chan event, getSize(setChSize)),
		evictCh:   make(chan event, getSize(evictChSize)),
	}
	m.wg.Add(5)
	go func() {
		for hitEvt := range m.hitCh {
			m.monitor.Hit(hitEvt.latency)
		}
		m.wg.Done()
	}()
	go func() {
		for missEvt := range m.missCh {
			m.monitor.Miss(missEvt.latency, missEvt.err)
		}
		m.wg.Done()
	}()
	go func() {
		for refreshEvt := range m.refreshCh {
			m.monitor.Refresh(refreshEvt.latency, refreshEvt.err)
		}
		m.wg.Done()
	}()
	go func() {
		for setEvt := range m.setCh {
			m.monitor.Set(setEvt.latency)
		}
		m.wg.Done()
	}()
	go func() {
		for _ = range m.evictCh {
			m.monitor.Evict()
		}
		m.wg.Done()
	}()
	return m
}

func getSize(sz *int) int {
	size := DefaultMonitorChannelSize
	if sz != nil {
		size = *sz
	}
	return size
}

func (dcm *ChannelBasedMonitor) Hit(latency time.Duration) {
	dcm.hitCh <- event{latency, nil}
}

func (dcm *ChannelBasedMonitor) Miss(latency time.Duration, err error) {
	dcm.missCh <- event{latency, err}
}

func (dcm *ChannelBasedMonitor) Refresh(latency time.Duration, err error) {
	dcm.refreshCh <- event{latency, err}
}

func (dcm *ChannelBasedMonitor) Set(latency time.Duration) {
	dcm.setCh <- event{latency, nil}
}

func (dcm *ChannelBasedMonitor) Evict() {
	dcm.evictCh <- event{}
}

func (dcm *ChannelBasedMonitor) Close() error {
	close(dcm.hitCh)
	close(dcm.missCh)
	close(dcm.refreshCh)
	close(dcm.setCh)
	close(dcm.evictCh)
	dcm.wg.Wait()
	return dcm.monitor.Close()
}
