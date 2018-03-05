package huobipro

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/bitly/go-simplejson"
	"github.com/ftao/ssmdb/exchanges/common"
	"io/ioutil"
)

/// 解压gzip的数据
func unGzipData(buf []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r)
}

// Endpoint 行情的Websocket入口
var Endpoint = "wss://api.huobi.pro/ws"

type HuobiproHandler struct{}

// NewMarket 创建Market实例
func NewHandler() *HuobiproHandler {
	return &HuobiproHandler{}
}

func (h *HuobiproHandler) GetEndpoint() string {
	return Endpoint
}

func hasKey(data *simplejson.Json, key string) bool {
	_, ok := data.CheckGet(key)
	return ok
}

func (h *HuobiproHandler) ParsePayload(data []byte) ([]*simplejson.Json, error) {
	msg, err := unGzipData(data)
	if err != nil {
		return nil, err
	}
	json, err := simplejson.NewJson(msg)
	if err != nil {
		return nil, err
	}
	return []*simplejson.Json{json}, nil
}

func (h *HuobiproHandler) ParseMsgType(msg *simplejson.Json) common.MsgType {
	switch {
	case hasKey(msg, "ping"):
		return common.PING
	case hasKey(msg, "pong"):
		return common.PONG
	case hasKey(msg, "ch"):
		return common.SUB_MSG
	case hasKey(msg, "status") && hasKey(msg, "id"):
		return common.SUB_REP
	default:
		return common.UNKNOW
	}
}

func (h *HuobiproHandler) RequireSubRep() bool {
	return true
}

func (h *HuobiproHandler) ParsePing(msg *simplejson.Json) int64 {
	return msg.Get("ping").MustInt64()
}

func (h *HuobiproHandler) ParsePong(msg *simplejson.Json) int64 {
	return msg.Get("pong").MustInt64()
}

func (h *HuobiproHandler) MakePong(t int64) *simplejson.Json {
	pong := simplejson.New()
	pong.Set("pong", t)
	return pong
}

func (h *HuobiproHandler) MakePing(t int64) *simplejson.Json {
	ping := simplejson.New()
	ping.Set("ping", t)
	return ping
}

func (h *HuobiproHandler) ParseSubRepId(msg *simplejson.Json) string {
	return msg.Get("id").MustString()
}

func (h *HuobiproHandler) ParseSubRepError(msg *simplejson.Json) string {
	return msg.Get("err-msg").MustString()
}

func (h *HuobiproHandler) ParseSubMsgTopic(msg *simplejson.Json) string {
	return msg.Get("ch").MustString()
}

func (h *HuobiproHandler) MakeSubReq(topic string) *simplejson.Json {
	req := simplejson.New()
	req.Set("id", topic)
	req.Set("sub", topic)
	return req
}

func (h *HuobiproHandler) MakeTopic(base string, dst string, dtype string) string {
	return fmt.Sprintf("market.%s%s.%s", base, dst, dtype)
}

func (h *HuobiproHandler) GetDataTypes() []string {
	return []string{
		"kline.1min",
		"depth.step0",
		"trade.detail",
		"detail",
	}
}
