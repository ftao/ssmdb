package gdax

import (
	"encoding/json"

	"github.com/ftao/ssmdb/exchanges/common"
	"github.com/gorilla/websocket"
)

var endpoint = "wss://ws-feed.gdax.com"

type GdaxExchange struct {
	name  string
	stats *common.StatsVars
	wss   *common.WebSocketSubscriber
	h     *GdaxHandler
}

func makeSubMessages(h *GdaxHandler) []common.WebSocketMessage {
	msgs := make([]common.WebSocketMessage, 0)
	b, err := json.Marshal(h.MakeSubReq())
	if err != nil {
		panic(err)
	}
	msg := common.WebSocketMessage{
		websocket.TextMessage,
		b,
	}
	msgs = append(msgs, msg)
	return msgs
}

func NewExchange(name string) *GdaxExchange {
	handler := NewHandler()
	subReqs := makeSubMessages(handler)
	factory := &common.SimpleWebSocketFacotry{
		Url:         endpoint,
		ReqHeader:   nil,
		SubRequests: subReqs,
	}

	wss := common.NewWebSocketSubscriber(factory)
	return &GdaxExchange{
		name:  name,
		stats: common.NewStatsVars(name),
		wss:   wss,
		h:     handler,
	}
}

func (m *GdaxExchange) Recv() ([]common.Message, error) {
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

func (m *GdaxExchange) Close() error {
	return m.wss.Close()
}
