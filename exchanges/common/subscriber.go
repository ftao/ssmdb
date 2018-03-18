package common

import (
	"log"
	"net/http"
	"sync"

	"github.com/cenkalti/backoff"
	"github.com/gorilla/websocket"
)

type WebSocketMessage struct {
	MessageType int
	Data        []byte
}

type IWriteMessage interface {
	WriteMessage(msgType int, data []byte) error
}

type WebSocketFactory interface {
	Connect() (*websocket.Conn, error)
	Init(conn IWriteMessage) error
}

type SimpleWebSocketFacotry struct {
	Url         string
	ReqHeader   http.Header
	SubRequests []WebSocketMessage
}

func (swf *SimpleWebSocketFacotry) Connect() (*websocket.Conn, error) {
	log.Printf("connect to %s", swf.Url)
	conn, resp, err := websocket.DefaultDialer.Dial(swf.Url, swf.ReqHeader)
	if err != nil {
		log.Printf("fail to connect, err=%s resp=%s", err, resp)
		return nil, err
	}
	return conn, nil
}

func (swf *SimpleWebSocketFacotry) Init(conn IWriteMessage) error {
	for _, msg := range swf.SubRequests {
		log.Printf("subscribe: %s", msg.Data)
		err := conn.WriteMessage(msg.MessageType, msg.Data)
		if err != nil {
			return err
		}
	}
	return nil
}

// The WebSocketSubscriber type represents read-only datasource
type WebSocketSubscriber struct {
	factory WebSocketFactory
	topics  *sync.Map
	//writeQueue chan *message
	readQueue chan *WebSocketMessage

	conn        *websocket.Conn
	isConnected bool
	connLock    *sync.Mutex
	connCond    *sync.Cond
	writeLock   sync.Mutex
}

func NewWebSocketSubscriber(factory WebSocketFactory) *WebSocketSubscriber {
	lock := &sync.Mutex{}
	ws := &WebSocketSubscriber{
		factory:  factory,
		connLock: lock,
		connCond: sync.NewCond(lock),
		//writeQueue: make(chan *message, 100),
		readQueue: make(chan *WebSocketMessage, 100),
	}
	go ws.keepConnected()
	go ws.readLoop()
	return ws
}

func (ws *WebSocketSubscriber) keepConnected() {
	for {
		ws.connLock.Lock()
		for ws.isConnected {
			ws.connCond.Wait()
		}
		ws.connect()
		ws.connCond.Broadcast()
		ws.connLock.Unlock()
		ws.subscribe()
	}
}

func (ws *WebSocketSubscriber) connect() {
	op := func() error {
		conn, err := ws.factory.Connect()
		if err != nil {
			return err
		} else {
			ws.conn = conn
			ws.isConnected = true
		}
		return nil
	}
	ebf := backoff.NewExponentialBackOff()
	ebf.MaxElapsedTime = 0

	err := backoff.Retry(op, ebf)
	if err != nil {
		log.Printf("connect fail: %s", err)
	} else {
		log.Printf("connect success")
	}
}

func (ws *WebSocketSubscriber) subscribe() {
	//TODO: handle error
	err := ws.factory.Init(ws)
	if err != nil {
		log.Printf("subscribe error: %s", err)
	} else {
		log.Printf("subscribe success")
	}
}

func (ws *WebSocketSubscriber) readLoop() {
	for {
		ws.waitUntilConnected()
		log.Printf("start read loop")
		for {
			mt, data, err := ws.conn.ReadMessage()
			if err != nil {
				log.Printf("err %s", err)
				ws.setNotConnected()
				break
			} else {
				ws.readQueue <- &WebSocketMessage{mt, data}
			}
		}
	}
}

func (ws *WebSocketSubscriber) ReadMessage() *WebSocketMessage {
	return <-ws.readQueue
}

func (ws *WebSocketSubscriber) WriteMessage(msgType int, data []byte) error {
	ws.writeLock.Lock()
	defer ws.writeLock.Unlock()
	err := ws.conn.WriteMessage(msgType, data)
	return err
}

func (ws *WebSocketSubscriber) Close() error {
	return ws.conn.Close()
}

func (ws *WebSocketSubscriber) setNotConnected() {
	log.Printf("set not connected")
	ws.connLock.Lock()
	ws.isConnected = false
	ws.conn = nil
	ws.connCond.Broadcast()
	ws.connLock.Unlock()
}

func (ws *WebSocketSubscriber) waitUntilConnected() {
	ws.connLock.Lock()
	for !ws.isConnected {
		ws.connCond.Wait()
	}
	ws.connLock.Unlock()
}
