package ttlmap

import (
	"testing"
	"time"
)

func TestNewItem(t *testing.T) {
	ttl := 1 * time.Second
	expiration := time.Now().Add(ttl)
	item := NewItem("foo", expiration)
	if item == nil {
		t.Fatalf("Expecting item")
	}
	if item.Value() != "foo" {
		t.Fatalf("Invalid value")
	}
	diff := item.TTL() - ttl
	if diff < 0 {
		diff = -diff
	}
	if diff > 10*time.Millisecond {
		t.Fatalf("Invalid TTL")
	}
	if item.Expiration() != expiration {
		t.Fatalf("Invalid expiration")
	}
	if item.Expired() {
		t.Fatalf("Expecting not expired")
	}
	<-time.After(time.Duration(float64(ttl) * 0.8))
	if item.Expired() {
		t.Fatalf("Expecting not expired")
	}
	<-time.After(time.Duration(float64(ttl) * 0.4))
	if !item.Expired() {
		t.Fatalf("Expecting expired")
	}
}

func TestNewItemWithTTL(t *testing.T) {
	ttl := 10 * time.Second
	expectedExpiration := time.Now().Add(ttl)
	item := NewItemWithTTL("foo", ttl)
	if item == nil {
		t.Fatalf("Expecting item")
	}
	diff := item.Expiration().Sub(expectedExpiration)
	if diff < 0 {
		diff = -diff
	}
	if diff > 10*time.Millisecond {
		t.Fatalf("Invalid expiration")
	}
}
