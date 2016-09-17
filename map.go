// Package ttlmap provides a map-like interface with string keys and expirable
// items. Keys are currently limited to strings.
package ttlmap

import (
	"container/heap"
	"errors"
	"sync"
	"time"
)

// Errors returned by Set and SetNX operations.
var (
	ErrExists  = errors.New("item already exists")
	ErrDrained = errors.New("map was drained")
)

// Options for initializing a Map.
type Options struct {
	InitialCapacity int
	OnWillExpire    func(key string, item *Item)
	OnWillEvict     func(key string, item *Item)
}

// Map is the equivalent of a map[string]interface{} but with expirable Items.
type Map struct {
	lock         sync.RWMutex
	m            map[string]*pqitem
	pq           pqueue
	updating     bool
	drained      bool
	onWillExpire func(key string, item *Item)
	onWillEvict  func(key string, item *Item)

	updateChan   chan struct{}
	drainChan    chan struct{}
	drainingChan chan struct{}
	doneChan     chan struct{}
}

// New creates a new Map with given options.
func New(options *Options) *Map {
	if options == nil {
		options = &Options{}
	}
	m := &Map{
		m:            make(map[string]*pqitem, options.InitialCapacity),
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

// Len returns the number of elements in the map.
func (m *Map) Len() int {
	m.lock.RLock()
	n := len(m.m)
	m.lock.RUnlock()
	return n
}

// Get returns the item in the map given its key.
func (m *Map) Get(key string) *Item {
	m.lock.RLock()
	if m.drained {
		m.lock.RUnlock()
		return nil
	}
	pqi := m.m[key]
	m.lock.RUnlock()
	if pqi != nil {
		return pqi.item
	}
	return nil
}

// Set assigns an expirable Item with the specified key in the map.
// ErrDrained will be returned if the map is already drained.
func (m *Map) Set(key string, item *Item) error {
	m.lock.Lock()
	if m.drained {
		m.lock.Unlock()
		return ErrDrained
	}
	if pqi := m.m[key]; pqi != nil {
		if !m.tryExpire(key, pqi) {
			m.evict(key, pqi)
		}
	}
	m.set(key, item)
	m.lock.Unlock()
	return nil
}

// SetNX assigns an expirable Item with the specified key in the map, only if
// the key is not already being in use.
// ErrExists will be returned if the key already exists.
// ErrDrained will be returned if the map is already drained.
func (m *Map) SetNX(key string, item *Item) error {
	m.lock.Lock()
	if m.drained {
		m.lock.Unlock()
		return ErrDrained
	}
	if pqi := m.m[key]; pqi != nil {
		m.lock.Unlock()
		return ErrExists
	}
	m.set(key, item)
	m.lock.Unlock()
	return nil
}

// Delete deletes the item with the specified key in the map.
// If an item is found, it is returned.
func (m *Map) Delete(key string) *Item {
	m.lock.Lock()
	if m.drained {
		m.lock.Unlock()
		return nil
	}
	if pqi := m.m[key]; pqi != nil {
		m.delete(key, pqi.index)
		m.lock.Unlock()
		return pqi.item
	}
	m.lock.Unlock()
	return nil
}

// Draining returns the channel that is closed when the map starts draining.
func (m *Map) Draining() <-chan struct{} {
	return m.drainingChan
}

// Drain evicts all remaining elements from the map and terminates the usage of
// this map.
func (m *Map) Drain() {
	select {
	case m.drainChan <- struct{}{}:
		close(m.drainingChan)
	default:
	}
	<-m.doneChan
}

func (m *Map) set(key string, item *Item) {
	pqi := &pqitem{
		key:   key,
		item:  item,
		index: -1,
	}
	m.m[key] = pqi
	heap.Push(&m.pq, pqi)
	if pqi.index == 0 {
		m.signalUpdate()
	}
}

func (m *Map) delete(key string, index int) {
	delete(m.m, key)
	heap.Remove(&m.pq, index)
	if index == 0 {
		m.signalUpdate()
	}
}

func (m *Map) tryExpire(key string, pqi *pqitem) bool {
	if pqi.item.Expired() {
		if m.onWillExpire != nil {
			m.onWillExpire(key, pqi.item)
		}
		m.evict(key, pqi)
		return true
	}
	return false
}

func (m *Map) evict(key string, pqi *pqitem) {
	if m.onWillEvict != nil {
		m.onWillEvict(key, pqi.item)
	}
	m.delete(key, pqi.index)
}

func (m *Map) signalUpdate() {
	if !m.updating {
		m.updating = true
		select {
		case m.updateChan <- struct{}{}:
		default:
		}
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
			m.update(timer, false)
		case <-timer.C:
			m.update(timer, true)
		}
	}
}

func (m *Map) update(timer *time.Timer, evict bool) {
	m.lock.Lock()
	if evict {
		m.evictExpired()
	}
	m.updating = false
	duration, ok := m.nextTTL()
	m.lock.Unlock()
	if ok {
		timer.Reset(duration)
	} else {
		timer.Stop()
	}
}

func (m *Map) nextTTL() (time.Duration, bool) {
	pqi := m.pq.peek()
	if pqi == nil {
		return 0, false
	}
	duration := pqi.item.TTL()
	if duration < 0 {
		duration = 0
	}
	return duration, true
}

func (m *Map) evictExpired() {
	for pqi := m.pq.peek(); pqi != nil; {
		if !m.tryExpire(pqi.key, pqi) {
			break
		}
		pqi = m.pq.peek()
	}
}

func (m *Map) drain() {
	m.lock.Lock()
	m.drained = true
	for pqi := m.pq.peek(); pqi != nil; {
		m.evict(pqi.key, pqi)
		pqi = m.pq.peek()
	}
	m.lock.Unlock()
}
