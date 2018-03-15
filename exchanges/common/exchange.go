package common

import (
	"github.com/bitly/go-simplejson"
)

type Message struct {
	Topic string
	Data  *simplejson.Json
}

type IExchange interface {
	Recv() ([]Message, error)
	Close() error
}
