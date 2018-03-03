package main

import (
	"fmt"
	"github.com/bitly/go-simplejson"
	"github.com/ftao/ssmdb/storage"
	"github.com/leizongmin/huobiapi"
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
	// 创建客户端实例
	market, err := huobiapi.NewMarket()
	if err != nil {
		panic(err)
	}

	symbols := []string{
		"btc",
		"bch",
		"eth",
		"etc",
		"ltc",
		"eos",
		"xrp",
		"omg",
		"dash",
		"zec",
	}

	dataTypes := []string{
		"kline.1min",
		"depth.step0",
		"trade.detail",
		"detail",
	}

	msg_ch := make(chan Msg, 1)
	go func() {
		store := storage.NewFsStore("data")
		for msg := range msg_ch {
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
		}
	}()

	sig_ch := make(chan os.Signal, 1)
	signal.Notify(sig_ch, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sig_ch
		log.Printf("recv signal %s", sig)
		msg_ch <- Msg{"__EXIT__", nil}
	}()

	// 订阅主题
	for _, symbol := range symbols {
		for _, dtype := range dataTypes {
			topic := fmt.Sprintf("market.%susdt.%s", symbol, dtype)
			// 收到数据更新时回调
			market.Subscribe(topic, func(topic string, json *huobiapi.JSON) {
				msg_ch <- Msg{topic, json}
			})
		}
	}
	log.Printf("finish subscribe")

	// 进入阻塞等待，这样不会导致进程退出
	market.Loop()
}
