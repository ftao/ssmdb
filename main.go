package main

import (
	"flag"
	"github.com/bitly/go-simplejson"
	"github.com/ftao/ssmdb/exchanges/common"
	"github.com/ftao/ssmdb/exchanges/huobipro"
	"github.com/ftao/ssmdb/exchanges/okex"
	"github.com/ftao/ssmdb/storage"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type Msg struct {
	topic string
	data  *simplejson.Json
}

func main() {
	exchange := flag.String("exchange", "huobipro", "exchange name")
	saveDir := "data3/" + *exchange
	var handler common.Handler
	switch *exchange {
	case "huobipro":
		handler = huobipro.NewHandler()
	case "okex":
		handler = okex.NewHandler()
	default:
		panic("invalid exchange name")
	}

	// 创建客户端实例
	//handler := okex.NewHandler()
	market, err := common.NewMarket(handler)
	if err != nil {
		panic(err)
	}

	log.Printf("fetch data from exchange %s, save to %s", *exchange, saveDir)

	msgCh := make(chan Msg, 1)

	counter := make(map[string]uint64)
	go func() {
		store := storage.NewFsStore(saveDir)
		idx := 0
		for msg := range msgCh {
			if msg.topic == "__EXIT__" {
				log.Printf("recv exit signal, close all files and exit")
				store.Close()
				os.Exit(0)
				break
			}
			err := store.Insert(msg.topic, msg.data)
			if err != nil {
				panic(err)
			}
			counter[msg.topic] += 1
			idx += 1
			if idx%1000 == 0 {
				for k, v := range counter {
					log.Printf("count %s=%d", k, v)
				}
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		log.Printf("recv signal %s", sig)
		msgCh <- Msg{"__EXIT__", nil}
	}()

	// 订阅主题
	for _, topic := range handler.GetTopics() {
		// topic := fmt.Sprintf("market.%susdt.%s", symbol, dtype)
		// 收到数据更新时回调
		market.Subscribe(topic, func(topic string, json *simplejson.Json) {
			msgCh <- Msg{topic, json}
		})
		log.Printf("subscribe %s", topic)
	}
	log.Printf("finish subscribe")

	// 进入阻塞等待，这样不会导致进程退出
	market.Loop()
}
