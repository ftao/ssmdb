package main

import (
	"fmt"
	"github.com/bitly/go-simplejson"
	"github.com/ftao/ssmdb/storage"
	"github.com/leizongmin/huobiapi"
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

	ch := make(chan Msg)
	go func() {
		store := storage.NewFsStore("data")
		for msg := range ch {
			err := store.Insert(msg.topic, msg.data)
			if err != nil {
				panic(err)
			}
		}
	}()

	// 订阅主题
	for _, symbol := range symbols {
		for _, dtype := range dataTypes {
			topic := fmt.Sprintf("market.%susdt.%s", symbol, dtype)
			// 收到数据更新时回调
			market.Subscribe(topic, func(topic string, json *huobiapi.JSON) {
				ch <- Msg{topic, json}
			})
		}
	}

	// 进入阻塞等待，这样不会导致进程退出
	market.Loop()
}
