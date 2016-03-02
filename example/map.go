package main

import (
	"fmt"
	"time"

	"github.com/imkira/go-ttlmap"
)

func main() {
	options := &ttlmap.Options{
		InitialCapacity: 1024,
		OnWillExpire: func(key string, item *ttlmap.Item) {
			fmt.Printf("expired: [%s=%v]\n", key, item.Value())
		},
		OnWillEvict: func(key string, item *ttlmap.Item) {
			fmt.Printf("evicted: [%s=%v]\n", key, item.Value())
		},
	}
	m := ttlmap.New(options)
	// don't forget to drain the map when you don't need it
	defer m.Drain()

	m.Set("foo", ttlmap.NewItemWithTTL("hello", 1000*time.Millisecond))
	m.Set("bar", ttlmap.NewItemWithTTL("world", 800*time.Millisecond))

	printStatus(m, "foo")
	printStatus(m, "bar")

	sleep(500 * time.Millisecond)

	printStatus(m, "foo")
	printStatus(m, "bar")

	sleep(1000 * time.Millisecond)

	printStatus(m, "foo")
	printStatus(m, "bar")
}

func printStatus(m *ttlmap.Map, key string) {
	item := m.Get(key)
	if item != nil {
		fmt.Printf("status: [%s=%v] will expire in: %v\n", key, item.Value(), item.TTL())
	} else {
		fmt.Printf("status: [%s] not in map anymore\n", key)
	}
}

func sleep(duration time.Duration) {
	fmt.Printf("Sleeping %v...\n", duration)
	time.Sleep(500 * time.Millisecond)
	fmt.Printf("Finished sleeping!\n")
}
