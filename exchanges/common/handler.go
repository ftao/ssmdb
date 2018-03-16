package common

import (
	"github.com/bitly/go-simplejson"
)

type Handler interface {
	ParsePayload(data []byte) ([]*simplejson.Json, error)
	MakePing(t int64) *simplejson.Json
	ParsePing(msg *simplejson.Json) int64
	MakePong(t int64) *simplejson.Json
	ParsePong(msg *simplejson.Json) int64
	RequireSubRep() bool
	MakeSubReq(topic string) *simplejson.Json
	ParseSubRepId(msg *simplejson.Json) string
	ParseSubRepError(msg *simplejson.Json) string
	ParseSubMsgTopic(msg *simplejson.Json) string
	GetDataTypes() []string
	MakeTopic(base string, dst string, dtype string) string
	GetTopics() []string
}
