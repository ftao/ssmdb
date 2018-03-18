package gdax

import (
	"github.com/bitly/go-simplejson"
	"github.com/ftao/ssmdb/exchanges/common"
)

type SubReq struct {
	Type       string   `json:"type"`
	ProductIds []string `json:"product_ids"`
	Channels   []string `json:"channels"`
}

type GdaxHandler struct{}

// NewMarket 创建Market实例
func NewHandler() *GdaxHandler {
	return &GdaxHandler{}
}

func (h *GdaxHandler) ParsePayload(data []byte) ([]common.Message, error) {
	json, err := simplejson.NewJson(data)
	if err != nil {
		return nil, err
	}
	product_id := json.Get("product_id").MustString("_unknown_")
	type_ := json.Get("type").MustString("_unknown_")
	topic := product_id + "." + type_
	return []common.Message{common.Message{topic, json}}, nil
}

func (h *GdaxHandler) MakeSubReq() interface{} {
	channels := []string{
		"heartbeat",
		"ticker",
		"level2",
		"matches",
		"full",
	}

	symbols := []string{
		"BCH-BTC",
		"BCH-USD",
		"BTC-EUR",
		"BTC-GBP",
		"BTC-USD",
		"ETH-BTC",
		"ETH-EUR",
		"ETH-USD",
		"LTC-BTC",
		"LTC-EUR",
		"LTC-USD",
		"BCH-EUR",
	}

	return &SubReq{
		Type:       "subscribe",
		ProductIds: symbols,
		Channels:   channels,
	}
}
