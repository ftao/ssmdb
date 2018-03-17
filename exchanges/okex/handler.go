package okex

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/bitly/go-simplejson"
	"github.com/ftao/ssmdb/exchanges/common"
)

type MsgType int

type SubReq struct {
	Event   string `json:"event"`
	Channel string `json:"channel"`
}

const (
	PING    MsgType = iota
	PONG            = iota
	SUB_MSG         = iota
)

func uncompress(buf []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r)
}

type OkexHandler struct{}

func NewHandler() *OkexHandler {
	return &OkexHandler{}
}

func (h *OkexHandler) ParsePayload(data []byte) ([]common.Message, error) {
	//msg, err := uncompress(data)
	//if err != nil {
	//		return nil, err
	//	}
	json, err := simplejson.NewJson(data)
	if err != nil {
		return nil, err
	}

	msgs := make([]common.Message, 0, 1)
	arr, err := json.Array()
	if err != nil {
		topic := h.ParseSubMsgTopic(json)
		msgs = append(msgs, common.Message{topic, json})
		return msgs, nil
	}
	for i, _ := range arr {
		msgData := json.GetIndex(i)
		topic := h.ParseSubMsgTopic(msgData)
		msgs = append(msgs, common.Message{topic, msgData})
	}
	return msgs, nil
}

func (h *OkexHandler) ParseMsgType(msg *simplejson.Json) MsgType {
	event := msg.Get("event").MustString()
	if event == "ping" {
		return PING
	} else if event == "pong" {
		return PONG
	} else {
		return SUB_MSG
	}
}

func (h *OkexHandler) MakePong() *simplejson.Json {
	pong := simplejson.New()
	pong.Set("event", "pong")
	return pong
}

func (h *OkexHandler) ParseSubMsgTopic(msg *simplejson.Json) string {
	return msg.Get("channel").MustString()
}

func (h *OkexHandler) MakeSubReq(topics []string) interface{} {
	arr := make([]SubReq, 0, len(topics))
	for _, topic := range topics {
		arr = append(arr, SubReq{"addChannel", topic})
	}
	return arr
}

func (h *OkexHandler) MakeTopic(base string, dst string, dtype string) string {
	return fmt.Sprintf("ok_sub_spot_%s_%s_%s", base, dst, dtype)
}

func (h *OkexHandler) GetDataTypes() []string {
	return []string{
		"kline",
		"deals",
		"ticker",
		"depth",
	}
}

func (h *OkexHandler) GetSymbols() []string {
	return strings.Split(
		"ltc_btc eth_btc etc_btc bch_btc bt1_btc bt2_btc btg_btc qtum_btc hsr_btc neo_btc gas_btc "+
			"btc_usdt eth_usdt ltc_usdt etc_usdt bch_usdt "+
			"qtum_usdt hsr_usdt neo_usdt gas_usdt "+
			"etc_eth",
		" ",
	)
}

func (h *OkexHandler) GetTopics() []string {
	symbols := h.GetSymbols()
	dtypes := h.GetDataTypes()
	topics := make([]string, 0, len(symbols)*len(dtypes))
	for _, symbol := range symbols {
		for _, dtype := range dtypes {
			topics = append(
				topics,
				fmt.Sprintf("ok_sub_spot_%s_%s", symbol, dtype),
			)
		}
	}
	return topics
}
