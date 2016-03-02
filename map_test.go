package ttlmap

import (
	"fmt"
	"testing"
	"time"
)

type testItem struct {
	key       string
	item      *Item
	timestamp time.Time
}

func TestNewMap(t *testing.T) {
	options := &Options{}
	m := New(options)
	if m == nil {
		t.Fatalf("Expecting map")
	}
	defer m.Drain()
}

func TestNewMapWithoutOptions(t *testing.T) {
	m := New(nil)
	if m == nil {
		t.Fatalf("Expecting map")
	}
	defer m.Drain()
}

func TestMapGetEmpty(t *testing.T) {
	options := &Options{}
	m := New(options)
	defer m.Drain()
	if m.Get("invalid") != nil {
		t.Fatalf("Not expecting item")
	}
}

func TestMapSetGet(t *testing.T) {
	options := &Options{}
	m := New(options)
	defer m.Drain()
	foo := NewItemWithTTL("hello", 1*time.Second)
	if err := m.Set("foo", foo); err != nil {
		t.Fatal(err)
	}
	if item := m.Get("foo"); item != foo || item.Value() != "hello" {
		t.Fatalf("Invalid item")
	}
	bar := NewItemWithTTL("world", 1*time.Second)
	if err := m.Set("bar", bar); err != nil {
		t.Fatal(err)
	}
	if item := m.Get("bar"); item != bar || bar.Value() != "world" {
		t.Fatalf("Invalid item")
	}
}

func TestMapSetNXGet(t *testing.T) {
	options := &Options{}
	m := New(options)
	defer m.Drain()
	foo := NewItemWithTTL("hello", 1*time.Second)
	if err := m.SetNX("foo", foo); err != nil {
		t.Fatal(err)
	}
	if item := m.Get("foo"); item != foo || item.Value() != "hello" {
		t.Fatalf("Invalid item")
	}
	bar := NewItemWithTTL("world", 1*time.Second)
	if err := m.SetNX("bar", bar); err != nil {
		t.Fatal(err)
	}
	if item := m.Get("bar"); item != bar || bar.Value() != "world" {
		t.Fatalf("Invalid item")
	}
	bar2 := NewItemWithTTL("world2", 1*time.Second)
	if err := m.SetNX("bar", bar2); err != ErrExists {
		t.Fatal(err)
	}
	if item := m.Get("bar"); item != bar || bar.Value() != "world" {
		t.Fatalf("Invalid item")
	}
}

func TestMapSetDeleteGet(t *testing.T) {
	options := &Options{}
	m := New(options)
	defer m.Drain()
	foo := NewItemWithTTL("hello", 1*time.Second)
	if err := m.Set("foo", foo); err != nil {
		t.Fatal(err)
	}
	if item := m.Get("foo"); item != foo || item.Value() != "hello" {
		t.Fatalf("Invalid item")
	}
	if m.Len() != 1 {
		t.Fatalf("Invalid length")
	}
	if item := m.Delete("foo"); item != foo {
		t.Fatalf("Invalid item")
	}
	if m.Len() != 0 {
		t.Fatalf("Invalid length")
	}
	if item := m.Get("foo"); item != nil {
		t.Fatalf("Not expecting item")
	}
	if item := m.Delete("foo"); item != nil {
		t.Fatalf("Not expecting item")
	}
}

func TestMapWaitExpired(t *testing.T) {
	var expired []*testItem
	options := &Options{
		OnWillExpire: func(key string, item *Item) {
			expired = append(expired, &testItem{key, item, time.Now()})
		},
	}
	m := New(options)
	defer m.Drain()
	start := time.Now()
	min := 500
	n := 100
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%d", i)
		value := fmt.Sprintf("value for %s", key)
		ttl := time.Duration(i+min) * time.Millisecond
		expiration := start.Add(ttl)
		item := NewItem(value, expiration)
		if err := m.SetNX(key, item); err != nil {
			t.Fatal(err)
		}
	}
	if m.Len() != n {
		t.Fatalf("Invalid length")
	}
	time.Sleep(1 * time.Second)
	if m.Len() != 0 {
		t.Fatalf("Invalid length")
	}
	m.Drain()
	if len(expired) != n {
		t.Fatalf("Expecting %d expired items\n", n)
	}
	for i, eitem := range expired {
		diff := eitem.timestamp.Sub(start)
		diff -= time.Duration(i+min) * time.Millisecond
		key := fmt.Sprintf("%d", i)
		if eitem.key != key {
			t.Fatalf("Wrong expiration key")
		}
		value := fmt.Sprintf("value for %s", key)
		if eitem.item.Value() != value {
			t.Fatalf("Wrong expiration value")
		}
		if diff < 0 || diff > 10*time.Millisecond {
			t.Fatalf("Wrong expiration time")
		}
	}
}

func TestMapDrain(t *testing.T) {
	options := &Options{}
	m := New(options)
	defer m.Drain()
	n := 100
	for i := 0; i < n; i++ {
		item := NewItemWithTTL("value", 100*time.Millisecond)
		m.Set(fmt.Sprintf("%d", i), item)
	}
	if m.Len() != 100 {
		t.Fatalf("Invalid length")
	}
	select {
	case <-m.Draining():
		t.Fatalf("Expecting not draining")
	default:
	}
	m.Drain()
	select {
	case <-m.Draining():
	default:
		t.Fatalf("Expecting draining")
	}
	if m.Len() != 0 {
		t.Fatalf("Invalid length")
	}
	if m.Get("1") != nil {
		t.Fatalf("Not expecting item")
	}
	item := NewItemWithTTL("value", 100*time.Millisecond)
	if err := m.Set("1", item); err != ErrDrained {
		t.Fatal(err)
	}
	if err := m.SetNX("1", item); err != ErrDrained {
		t.Fatal(err)
	}
	if item := m.Delete("1"); item != nil {
		t.Fatalf("Not expecting item")
	}
}

func TestMapSetSetEvict(t *testing.T) {
	var evicted []*testItem
	options := &Options{
		OnWillEvict: func(key string, item *Item) {
			evicted = append(evicted, &testItem{key, item, time.Now()})
		},
	}
	m := New(options)
	defer m.Drain()
	item := NewItemWithTTL("hello", 1*time.Second)
	if err := m.Set("foo", item); err != nil {
		t.Fatal(err)
	}
	if len(evicted) != 0 {
		t.Fatalf("Invalid length")
	}
	item = NewItemWithTTL("world", 2*time.Second)
	if err := m.Set("foo", item); err != nil {
		t.Fatal(err)
	}
	if len(evicted) != 1 {
		t.Fatalf("Invalid length")
	}
}

func TestMapExpireAlreadyExpired(t *testing.T) {
	var expired []*testItem
	options := &Options{
		OnWillExpire: func(key string, item *Item) {
			expired = append(expired, &testItem{key, item, time.Now()})
		},
	}
	m := New(options)
	defer m.Drain()
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	expiration := start.Add(-1 * time.Second)
	item := NewItem("bar", expiration)
	if err := m.Set("foo", item); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if m.Len() != 0 {
		t.Fatalf("Invalid length")
	}
	m.Drain()
	if len(expired) != 1 {
		t.Fatalf("Expecting %d expired items\n", 1)
	}
	eitem := expired[0]
	diff := eitem.timestamp.Sub(start)
	if diff < 0 || diff > 10*time.Millisecond {
		t.Fatalf("Wrong expiration time")
	}
}

func TestMapGetAlreadyExpired(t *testing.T) {
	options := &Options{}
	m := New(options)
	defer m.Drain()
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	expiration := start.Add(-1 * time.Second)
	done := false
	for i := 0; i < 1000 && !done; i++ {
		item := NewItem("bar", expiration)
		if err := m.Set("foo", item); err != nil {
			t.Fatal(err)
		}
		if item := m.Get("foo"); item != nil {
			done = true
			break
		}
		if m.Len() != 0 {
			t.Fatalf("Invalid length")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !done {
		t.Fatalf("Expecting get to succeed")
	}
}
