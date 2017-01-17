// Package ttlmap provides a map-like interface with string keys and expirable
// items. Keys are currently limited to strings.
package ttlmap

import "errors"

// Errors returned Map operations.
var (
	ErrNotExist = errors.New("key does not exist")
	ErrExist    = errors.New("key already exists")
	ErrDrained  = errors.New("map was drained")
)

// Map is the equivalent of a map[string]interface{} but with expirable Items.
type Map struct {
	store  *store
	keeper *keeper
}

// New creates a new Map with given options.
func New(opts *Options) *Map {
	if opts == nil {
		opts = &Options{}
	}
	store := newStore(opts)
	m := &Map{
		store:  store,
		keeper: newKeeper(store),
	}
	go m.keeper.run()
	return m
}

// Len returns the number of elements in the map.
func (m *Map) Len() int {
	m.store.RLock()
	n := len(m.store.kv)
	m.store.RUnlock()
	return n
}

// Get returns the item in the map given its key.
func (m *Map) Get(key string) *Item {
	m.store.RLock()
	if m.keeper.drained {
		m.store.RUnlock()
		return nil
	}
	pqi := m.store.kv[key]
	m.store.RUnlock()
	if pqi != nil {
		return pqi.item
	}
	return nil
}

// Set assigns an expirable Item with the specified key in the map.
// ErrExist or ErrNotExist may be returned depending on opts.KeyExist.
// ErrDrained will be returned if the map is already drained.
func (m *Map) Set(key string, item *Item, opts *SetOptions) error {
	m.store.Lock()
	if m.keeper.drained {
		m.store.Unlock()
		return ErrDrained
	}
	err := m.set(key, item, opts)
	m.store.Unlock()
	return err
}

// Delete deletes the item with the specified key in the map.
// If an item is found, it is returned.
func (m *Map) Delete(key string) *Item {
	m.store.Lock()
	if m.keeper.drained {
		m.store.Unlock()
		return nil
	}
	if pqi := m.store.kv[key]; pqi != nil {
		m.delete(pqi)
		m.store.Unlock()
		return pqi.item
	}
	m.store.Unlock()
	return nil
}

// Draining returns the channel that is closed when the map starts draining.
func (m *Map) Draining() <-chan struct{} {
	return m.keeper.drainingChan
}

// Drain evicts all remaining elements from the map and terminates the usage of
// this map.
func (m *Map) Drain() {
	m.keeper.signalDrain()
	<-m.keeper.doneChan
}

func (m *Map) set(key string, item *Item, opts *SetOptions) error {
	if pqi := m.store.kv[key]; pqi != nil {
		if opts.keyExist() == KeyExistNotYet {
			return ErrExist
		}
		m.expireOrEvict(pqi)
	} else if opts.keyExist() == KeyExistAlready {
		return ErrNotExist
	}
	pqi := &pqitem{
		key:   key,
		item:  item,
		index: -1,
	}
	m.store.set(pqi)
	if pqi.index == 0 {
		m.keeper.signalUpdate()
	}
	return nil
}

func (m *Map) expireOrEvict(pqi *pqitem) {
	if pqi.index == 0 {
		m.keeper.signalUpdate()
	}
	if !m.store.tryExpire(pqi) {
		m.store.evict(pqi)
	}
}

func (m *Map) delete(pqi *pqitem) {
	if pqi.index == 0 {
		m.keeper.signalUpdate()
	}
	m.store.delete(pqi)
}
