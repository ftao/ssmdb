package huobipro

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/bitly/go-simplejson"
	"github.com/ftao/ssmdb/exchanges/common"
	"io/ioutil"
)

type MsgType int

const (
	UNKNOW  MsgType = iota
	PING            = iota
	PONG            = iota
	SUB_REP         = iota
	SUB_MSG         = iota
)

/// 解压gzip的数据
func unGzipData(buf []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r)
}

type HuobiproHandler struct{}

// NewMarket 创建Market实例
func NewHandler() *HuobiproHandler {
	return &HuobiproHandler{}
}

func hasKey(data *simplejson.Json, key string) bool {
	_, ok := data.CheckGet(key)
	return ok
}

func (h *HuobiproHandler) ParsePayload(data []byte) ([]common.Message, error) {
	msg, err := unGzipData(data)
	if err != nil {
		return nil, err
	}
	json, err := simplejson.NewJson(msg)
	if err != nil {
		return nil, err
	}
	topic := json.Get("ch").MustString("_unknown_")
	return []common.Message{common.Message{topic, json}}, nil
}

func (h *HuobiproHandler) ParseMsgType(msg *simplejson.Json) MsgType {
	switch {
	case hasKey(msg, "ping"):
		return PING
	case hasKey(msg, "pong"):
		return PONG
	case hasKey(msg, "ch"):
		return SUB_MSG
	case hasKey(msg, "status") && hasKey(msg, "id"):
		return SUB_REP
	default:
		return UNKNOW
	}
}

func (h *HuobiproHandler) ParsePing(msg *simplejson.Json) int64 {
	return msg.Get("ping").MustInt64()
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

func (h *HuobiproHandler) GetTopics() []string {
	bases := []string{
		"usdt",
		"btc",
		"eth",
	}

	targets := make([][]string, 3)
	targets[0] = []string{
		"btc",
		"eth",
		"bch",
		"etc",
		"ltc",
		"eos",
		"xrp",
		"omg",
		"dash",
		"zec",
	}
	targets[1] = []string{
		"eth",
		"bch",
		"etc",
		"ltc",
		"eos",
		"xrp",
		"omg",
		"dash",
		"zec",
	}

	targets[2] = []string{
		"eos",
		"omg",
	}

	dataTypes := h.GetDataTypes()
	topics := make([]string, 0)
	for i, base := range bases {
		for _, target := range targets[i] {
			for _, dtype := range dataTypes {
				topics = append(
					topics,
					h.MakeTopic(target, base, dtype),
				)
			}
		}
	}
	return topics
}
