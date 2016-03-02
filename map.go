package ttlmap

import (
	"container/heap"
	"errors"
	"sync"
	"time"
)

var (
	ErrExists  = errors.New("item already exists")
	ErrDrained = errors.New("map was drained")
)

type Options struct {
	InitialCapacity int
	OnWillExpire    func(key string, item *Item)
	OnWillEvict     func(key string, item *Item)
}

type Map struct {
	lock         sync.RWMutex
	m            map[string]*Item
	pq           pqueue
	drained      bool
	onWillExpire func(key string, item *Item)
	onWillEvict  func(key string, item *Item)

	updateChan   chan struct{}
	drainChan    chan struct{}
	drainingChan chan struct{}
	doneChan     chan struct{}
}

func New(options *Options) *Map {
	if options == nil {
		options = &Options{}
	}
	m := &Map{
		m:            make(map[string]*Item, options.InitialCapacity),
		pq:           make(pqueue, 0, options.InitialCapacity),
		onWillExpire: options.OnWillExpire,
		onWillEvict:  options.OnWillEvict,
		updateChan:   make(chan struct{}, 1),
		drainChan:    make(chan struct{}, 1),
		drainingChan: make(chan struct{}),
		doneChan:     make(chan struct{}),
	}
	go m.run()
	return m
}

func (m *Map) Len() int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return len(m.m)
}

func (m *Map) Get(key string) *Item {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if m.drained {
		return nil
	}
	return m.m[key]
}

func (m *Map) Set(key string, item *Item) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.drained {
		return ErrDrained
	}
	item2 := m.m[key]
	if item2 != nil {
		if !m.tryExpire(key, item2) {
			m.evict(key, item2)
		}
	}
	m.set(key, item)
	return nil
}

func (m *Map) SetNX(key string, item *Item) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.drained {
		return ErrDrained
	}
	item2 := m.m[key]
	if item2 != nil {
		return ErrExists
	}
	m.set(key, item)
	return nil
}

func (m *Map) Delete(key string) *Item {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.drained {
		return nil
	}
	item := m.m[key]
	if item != nil {
		m.delete(key, item.index)
		return item
	}
	return nil
}

func (m *Map) Draining() <-chan struct{} {
	return m.drainingChan
}

func (m *Map) Drain() {
	select {
	case m.drainChan <- struct{}{}:
		close(m.drainingChan)
	default:
	}
	<-m.doneChan
}

func (m *Map) set(key string, item *Item) {
	m.m[key] = item
	pqi := &pqitem{
		key:  key,
		item: item,
	}
	heap.Push(&m.pq, pqi)
	m.signalChanges()
}

func (m *Map) delete(key string, index int) {
	delete(m.m, key)
	heap.Remove(&m.pq, index)
	m.signalChanges()
}

func (m *Map) tryExpire(key string, item *Item) bool {
	if item.Expired() {
		if m.onWillExpire != nil {
			m.onWillExpire(key, item)
		}
		m.evict(key, item)
		return true
	}
	return false
}

func (m *Map) evict(key string, item *Item) {
	if m.onWillEvict != nil {
		m.onWillEvict(key, item)
	}
	m.delete(key, item.index)
}

func (m *Map) signalChanges() {
	select {
	case m.updateChan <- struct{}{}:
	default:
	}
}

func (m *Map) run() {
	defer close(m.doneChan)
	defer m.drain()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-m.drainingChan:
			return
		case <-m.updateChan:
			m.updateTimer(timer)
		case <-timer.C:
			timer.Stop()
			m.evictExpired()
			m.updateTimer(timer)
		}
	}
}

func (m *Map) updateTimer(timer *time.Timer) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	if pqi, ok := m.pq.peek(); ok {
		duration := pqi.item.TTL()
		if duration < 0 {
			duration = 0
		}
		timer.Reset(duration)
	}
}

func (m *Map) evictExpired() {
	m.lock.Lock()
	defer m.lock.Unlock()
	for pqi, ok := m.pq.peek(); ok; {
		if !m.tryExpire(pqi.key, pqi.item) {
			break
		}
		pqi, ok = m.pq.peek()
	}
}

func (m *Map) drain() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.drained = true
	for pqi, ok := m.pq.peek(); ok; {
		m.evict(pqi.key, pqi.item)
		pqi, ok = m.pq.peek()
	}
}
