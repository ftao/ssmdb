package okex

import (
	"encoding/json"

	"github.com/ftao/ssmdb/exchanges/common"
	"github.com/gorilla/websocket"
)

var endpoint = "wss://real.okex.com:10441/websocket"

type OkexExchange struct {
	name  string
	stats *common.StatsVars
	wss   *common.WebSocketSubscriber
	h     *OkexHandler
}

func makeSubMessages(h *OkexHandler) []common.WebSocketMessage {
	msgs := make([]common.WebSocketMessage, 0)
	b, err := json.Marshal(h.MakeSubReq(h.GetTopics()))
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

func NewExchange(name string) *OkexExchange {
	handler := NewHandler()
	subReqs := makeSubMessages(handler)
	factory := &common.SimpleWebSocketFacotry{
		Url:         endpoint,
		ReqHeader:   nil,
		SubRequests: subReqs,
	}

	wss := common.NewWebSocketSubscriber(factory)
	return &OkexExchange{
		name:  name,
		stats: common.NewStatsVars(name),
		wss:   wss,
		h:     handler,
	}
}

func (m *OkexExchange) Recv() ([]common.Message, error) {
	wsMsg := m.wss.ReadMessage()
	msgs, err := m.h.ParsePayload(wsMsg.Data)
	if err != nil {
		return nil, err
	}
	toReturn := make([]common.Message, 0, len(msgs))
	for _, msg := range msgs {
		mt := m.h.ParseMsgType(msg.Data)
		switch mt {
		case PING:
			m.HandlePing(msg)
		default:
			toReturn = append(toReturn, msg)
		}
		m.stats.UpdateOnRecv(msg.Topic)
	}
	return toReturn, nil
}

func (m *OkexExchange) HandlePing(msg common.Message) error {
	pong, _ := json.Marshal(m.h.MakePong())
	m.stats.UpdateOnSend()
	return m.wss.WriteMessage(websocket.TextMessage, pong)
}

func (m *OkexExchange) Close() error {
	return m.wss.Close()
}
