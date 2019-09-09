package cache

type debugEvent struct {
	name string
}

func (c *PrefetchCache) sendDebugEvent(eventName string) {
	if c.debugEvents != nil {
		c.debugEvents <- debugEvent{name: eventName}
	}
}
