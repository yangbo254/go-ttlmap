package ttlmap

import "time"

type Item struct {
	value      interface{}
	expiration time.Time
	index      int
}

func NewItem(value interface{}, expiration time.Time) *Item {
	return &Item{
		value:      value,
		expiration: expiration,
		index:      -1,
	}
}

func NewItemWithTTL(value interface{}, duration time.Duration) *Item {
	return NewItem(value, time.Now().Add(duration))
}

func (item *Item) Value() interface{} {
	return item.value
}

func (item *Item) Expiration() time.Time {
	return item.expiration
}

func (item *Item) Expired() bool {
	return item.expiration.Before(time.Now())
}

func (item *Item) TTL() time.Duration {
	return item.expiration.Sub(time.Now())
}
