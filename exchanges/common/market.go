package common

import (
	"encoding/json"
	"fmt"
	"time"

	"log"
	"math"

	"github.com/bitly/go-simplejson"
	"sync"
)

type jsonChan = chan *simplejson.Json

// ConnectionClosedError Websocket未连接错误
var ConnectionClosedError = fmt.Errorf("websocket connection closed")

type wsOperation struct {
	cmd  string
	data interface{}
}

type Market struct {
	// Endpoint
	handler Handler
	ws      *SafeWebSocket

	listeners         *sync.Map
	subscribedTopic   map[string]bool
	subscribeResultCb map[string]jsonChan
	requestResultCb   map[string]jsonChan

	// 掉线后是否自动重连，如果用户主动执行Close()则不自动重连
	autoReconnect bool

	// 上次接收到的ping时间戳
	lastPing int64

	// 主动发送心跳的时间间隔，默认5秒
	HeartbeatInterval time.Duration
	// 接收消息超时时间，默认10秒
	ReceiveTimeout time.Duration
}

// Listener 订阅事件监听器
type Listener = func(topic string, json *simplejson.Json)

// NewMarket 创建Market实例
func NewMarket(h Handler) (m *Market, err error) {
	m = &Market{
		handler:           h,
		HeartbeatInterval: 5 * time.Second,
		ReceiveTimeout:    10 * time.Second,
		ws:                nil,
		autoReconnect:     true,
		listeners:         &sync.Map{},
		subscribeResultCb: make(map[string]jsonChan),
		requestResultCb:   make(map[string]jsonChan),
		subscribedTopic:   make(map[string]bool),
	}

	if err := m.connect(); err != nil {
		return nil, err
	}

	return m, nil
}

// connect 连接
func (m *Market) connect() error {
	log.Println("connecting")
	ws, err := NewSafeWebSocket(m.handler.GetEndpoint())
	if err != nil {
		return err
	}
	m.ws = ws
	m.lastPing = GetUinxMillisecond()
	log.Println("connected")

	m.handleMessageLoop()
	m.keepAlive()

	return nil
}

// reconnect 重新连接
func (m *Market) reconnect() error {
	log.Println("reconnecting after 1s")
	time.Sleep(time.Second)

	if err := m.connect(); err != nil {
		log.Println(err)
		return err
	}

	// 重新订阅
	var listeners = make(map[string]Listener)
	m.listeners.Range(func(key, value interface{}) bool {
		listeners[key.(string)] = value.(Listener)
		return true
	})
	for topic, listener := range listeners {
		delete(m.subscribedTopic, topic)
		m.Subscribe(topic, listener)
	}
	return nil
}

// sendMessage 发送消息
func (m *Market) sendMessage(data interface{}) error {
	b, err := json.Marshal(data)
	if err != nil {
		return nil
	}
	log.Println("sendMessage", string(b))
	m.ws.Send(b)
	return nil
}

// handleMessageLoop 处理消息循环
func (m *Market) handleMessageLoop() {
	m.ws.Listen(func(buf []byte) {
		msgs, err := m.handler.ParsePayload(buf)
		//log.Println("readMessage", string(msg))
		if err != nil {
			log.Println(err)
			return
		}
		for _, msg := range msgs {
			t := m.handler.ParseMsgType(msg)

			switch t {
			case PING:
				t := m.handler.ParsePing(msg)
				if t > 0 {
					m.sendMessage(m.handler.MakePong(t))
				}
			case PONG:
				//m.handler.HandlePong(json)
				t := m.handler.ParsePong(msg)
				if t > 0 {
					m.lastPing = t
				}
			case SUB_REP:
				id := m.handler.ParseSubRepId(msg)
				c, ok := m.subscribeResultCb[id]
				if ok {
					c <- msg
				}
			case SUB_MSG:
				ch := m.handler.ParseSubMsgTopic(msg)
				if ch != "" {
					listener, ok := m.listeners.Load(ch)
					if ok {
						// log.Println("handleSubscribe", json)
						listener.(Listener)(ch, msg)
					}
				}
			case UNKNOW:
				log.Printf("unknow message: %s", msg)
			}
		}
	})
}

// keepAlive 保持活跃
func (m *Market) keepAlive() {
	m.ws.KeepAlive(m.HeartbeatInterval, func() {
		var t = GetUinxMillisecond()
		m.sendMessage(m.handler.MakePing(t))

		// 检查上次ping时间，如果超过20秒无响应，重新连接
		tr := time.Duration(math.Abs(float64(t - m.lastPing)))
		if tr >= m.HeartbeatInterval*2 {
			log.Println("no ping max delay", tr, m.HeartbeatInterval*2, t, m.lastPing)
			if m.autoReconnect {
				err := m.reconnect()
				if err != nil {
					log.Println(err)
				}
			}
		}
	})
}

// Subscribe 订阅
func (m *Market) Subscribe(topic string, listener Listener) error {
	log.Println("subscribe", topic)

	var isNew = false

	// 如果未曾发送过订阅指令，则发送，并等待订阅操作结果，否则直接返回
	if _, ok := m.subscribedTopic[topic]; !ok {
		m.subscribeResultCb[topic] = make(jsonChan)
		m.sendMessage(m.handler.MakeSubReq(topic))
		isNew = true
	} else {
		log.Println("send subscribe before, reset listener only")
	}

	m.listeners.Store(topic, listener)
	m.subscribedTopic[topic] = true

	if isNew && m.handler.RequireSubRep() {
		var json = <-m.subscribeResultCb[topic]
		// 判断订阅结果，如果出错则返回出错信息
		if msg := m.handler.ParseSubRepError(json); msg != "" {
			return fmt.Errorf(msg)
		}
	}
	return nil
}

// Unsubscribe 取消订阅
func (m *Market) Unsubscribe(topic string) {
	log.Println("unSubscribe", topic)
	// 火币网没有提供取消订阅的接口，只能删除监听器
	m.listeners.Delete(topic)
}

// Loop 进入循环
func (m *Market) Loop() {
	log.Println("startLoop")
	for {
		err := m.ws.Loop()
		if err != nil {
			log.Println(err)
			if err == SafeWebSocketDestroyError {
				break
			} else if m.autoReconnect {
				m.reconnect()
			} else {
				break
			}
		}
	}
	log.Println("endLoop")
}

// ReConnect 重新连接
func (m *Market) ReConnect() (err error) {
	log.Println("reconnect")
	m.autoReconnect = true
	if err = m.ws.Destroy(); err != nil {
		return err
	}
	return m.reconnect()
}

// Close 关闭连接
func (m *Market) Close() error {
	log.Println("close")
	m.autoReconnect = false
	if err := m.ws.Destroy(); err != nil {
		return err
	}
	return nil
}
