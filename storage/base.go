package storage

import (
	"fmt"
	"github.com/bitly/go-simplejson"
)

type Store interface {
	Insert(topic string, data *simplejson.Json) error
	Close() error
}

type DummStore struct{}

func (s *DummStore) Insert(topic string, data *simplejson.Json) error {
	fmt.Printf("%s %s", topic, data)
	return nil
}
