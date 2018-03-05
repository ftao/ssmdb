package okex

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"github.com/bitly/go-simplejson"
	"github.com/ftao/ssmdb/exchanges/common"
	"io/ioutil"
)

func uncompress(buf []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r)
}

var Endpoint = "wss://real.okex.com:10441/websocket"

type OkExHandler struct{}

func NewHandler() *OkExHandler {
	return &OkExHandler{}
}

func (h *OkExHandler) GetEndpoint() string {
	return Endpoint
}

func (h *OkExHandler) ParsePayload(data []byte) ([]*simplejson.Json, error) {
	//msg, err := uncompress(data)
	//if err != nil {
	//		return nil, err
	//	}
	msg := data
	json, err := simplejson.NewJson(msg)
	if err != nil {
		return nil, err
	}
	msgs := make([]*simplejson.Json, 0, 1)
	arr, err := json.Array()
	if err != nil {
		msgs = append(msgs, json)
		return msgs, nil
	}
	for i, _ := range arr {
		msgs = append(msgs, json.GetIndex(i))
	}
	return msgs, nil
}

func (h *OkExHandler) ParseMsgType(msg *simplejson.Json) common.MsgType {
	event := msg.Get("event").MustString()
	if event == "ping" {
		return common.PING
	} else if event == "pong" {
		return common.PONG
	} else {
		return common.SUB_MSG
	}
	//return common.UNKNOW
}

func (h *OkExHandler) RequireSubRep() bool {
	return false
}

func (h *OkExHandler) ParsePing(msg *simplejson.Json) int64 {
	return common.GetUinxMillisecond()
}

func (h *OkExHandler) ParsePong(msg *simplejson.Json) int64 {
	return common.GetUinxMillisecond()
}

func (h *OkExHandler) MakePong(t int64) *simplejson.Json {
	pong := simplejson.New()
	pong.Set("event", "pong")
	return pong
}

func (h *OkExHandler) MakePing(t int64) *simplejson.Json {
	ping := simplejson.New()
	ping.Set("event", "ping")
	return ping
}

func (h *OkExHandler) ParseSubRepId(msg *simplejson.Json) string {
	return ""
}

func (h *OkExHandler) ParseSubRepError(msg *simplejson.Json) string {
	return ""
}

func (h *OkExHandler) ParseSubMsgTopic(msg *simplejson.Json) string {
	return msg.Get("channel").MustString()
}

func (h *OkExHandler) MakeSubReq(topic string) *simplejson.Json {
	req := simplejson.New()
	req.Set("event", "addChannel")
	req.Set("channel", topic)
	return req
}

func (h *OkExHandler) MakeTopic(base string, dst string, dtype string) string {
	return fmt.Sprintf("ok_sub_spot_%s_%s_%s", base, dst, dtype)
}

func (h *OkExHandler) GetDataTypes() []string {
	return []string{
		"kline",
		"deals",
		"ticker",
		"depth",
	}
}
