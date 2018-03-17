package binance

import (
	"fmt"

	"github.com/bitly/go-simplejson"
	"github.com/ftao/ssmdb/exchanges/common"
)

type BinanceHandler struct{}

// NewMarket 创建Market实例
func NewHandler() *BinanceHandler {
	return &BinanceHandler{}
}

func (h *BinanceHandler) ParsePayload(data []byte) ([]common.Message, error) {
	json, err := simplejson.NewJson(data)
	if err != nil {
		return nil, err
	}
	topic := json.Get("stream").MustString("_unknown_")
	return []common.Message{common.Message{topic, json}}, nil
}

func (h *BinanceHandler) ParseSubMsgTopic(msg *simplejson.Json) string {
	return msg.Get("stream").MustString()
}

func (h *BinanceHandler) MakeTopic(symbol string, dtype string) string {
	return fmt.Sprintf("%s@%s", symbol, dtype)
}

func (h *BinanceHandler) GetDataTypes() []string {
	return []string{
		"kline_1m",
		"ticker",
		"depth20",
		"trade",
	}
}

func (h *BinanceHandler) GetTopics() []string {

	symbols := []string{
		"btcusdt",
		"bccusdt",
		"ethusdt",
		"ltcusdt",
		"neousdt",

		"bccbtc",
		"ethbtc",
		"etcbtc",
		"neobtc",

		"etceth",
		"eoseth",
		"neoeth",
	}

	dataTypes := h.GetDataTypes()
	topics := make([]string, 0)
	for _, symbol := range symbols {
		for _, dtype := range dataTypes {
			topics = append(
				topics,
				h.MakeTopic(symbol, dtype),
			)
		}
	}
	return topics
}
