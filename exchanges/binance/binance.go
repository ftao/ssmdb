package binance

import (
	"strings"

	"github.com/ftao/ssmdb/exchanges/common"
)

var endpoint = "wss://stream.binance.com:9443/stream?streams="

type BinanceExchange struct {
	name  string
	stats *common.StatsVars
	wss   *common.WebSocketSubscriber
	h     *BinanceHandler
}

func NewExchange(name string) *BinanceExchange {
	handler := NewHandler()
	url := endpoint + strings.Join(handler.GetTopics(), "/")
	factory := &common.SimpleWebSocketFacotry{
		Url:       url,
		ReqHeader: nil,
	}

	wss := common.NewWebSocketSubscriber(factory)
	return &BinanceExchange{
		name:  name,
		stats: common.NewStatsVars(name),
		wss:   wss,
		h:     handler,
	}
}

func (m *BinanceExchange) Recv() ([]common.Message, error) {
	wsMsg := m.wss.ReadMessage()
	msgs, err := m.h.ParsePayload(wsMsg.Data)
	if err != nil {
		return nil, err
	}
	for _, msg := range msgs {
		m.stats.UpdateOnRecv(msg.Topic)
	}
	return msgs, nil
}

func (m *BinanceExchange) Close() error {
	return m.wss.Close()
}
