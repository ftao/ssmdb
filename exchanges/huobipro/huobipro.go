package huobipro

import (
	"encoding/json"
	"expvar"
	"log"

	"github.com/ftao/ssmdb/exchanges/common"
	"github.com/gorilla/websocket"
)

var endpoint = "wss://api.huobi.pro/ws"

type HuobiproExchange struct {
	name           string
	lastPong       *expvar.Int
	recvCounter    *expvar.Int
	counterByTopic *expvar.Map
	wss            *common.WebSocketSubscriber
	h              *HuobiproHandler
}

func makeSubMessages(h *HuobiproHandler) []common.WebSocketMessage {
	msgs := make([]common.WebSocketMessage, 0)
	for _, topic := range h.GetTopics() {
		b, err := json.Marshal(h.MakeSubReq(topic))
		if err != nil {
			panic(err)
		}
		msg := common.WebSocketMessage{
			websocket.TextMessage,
			b,
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

func NewExchange(name string) *HuobiproExchange {
	handler := NewHandler()
	subReqs := makeSubMessages(handler)
	factory := &common.SimpleWebSocketFacotry{
		Url:         endpoint,
		ReqHeader:   nil,
		SubRequests: subReqs,
	}

	wss := common.NewWebSocketSubscriber(factory)
	counterByTopic := expvar.NewMap(name + ".topic_msg_count")
	counterByTopic.Init()
	return &HuobiproExchange{
		name:           name,
		lastPong:       expvar.NewInt(name + ".last_pong"),
		recvCounter:    expvar.NewInt(name + ".msg_count"),
		counterByTopic: counterByTopic,
		wss:            wss,
		h:              handler,
	}
}

func (m *HuobiproExchange) Recv() ([]common.Message, error) {
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
		case SUB_REP:
			m.HandleSubRep(msg)
		default:
			m.counterByTopic.Add(msg.Topic, 1)
			toReturn = append(toReturn, msg)
		}
	}
	m.recvCounter.Add(1)
	return toReturn, nil
}

func (m *HuobiproExchange) HandlePing(msg common.Message) error {
	t := m.h.ParsePing(msg.Data)
	pong, _ := json.Marshal(m.h.MakePong(t))
	m.lastPong.Set(t)
	return m.wss.WriteMessage(websocket.TextMessage, pong)
}

func (m *HuobiproExchange) HandleSubRep(msg common.Message) error {
	log.Printf(
		"recv sub reply message: id=%s status=%s",
		msg.Data.Get("id").MustString(),
		msg.Data.Get("status").MustString(),
	)
	return nil
}

func (m *HuobiproExchange) Close() error {
	return m.wss.Close()
}
