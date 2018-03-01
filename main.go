package main

import (
	"bufio"
	"fmt"
	"github.com/leizongmin/huobiapi"
	"os"
)

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

	ch := make(chan *huobiapi.JSON)
	go func() {
		f, err := os.Create("result.jsonlines")
		if err != nil {
			panic(err)
		}
		defer f.Close()
		w := bufio.NewWriter(f)
		for json := range ch {
			bytes, err := json.MarshalJSON()
			if err != nil {
				fmt.Printf("error: %s", err)
			} else {
				w.Write(bytes)
				w.WriteByte('\n')
				w.Flush()
			}
		}
	}()

	// 订阅主题
	for _, symbol := range symbols {
		for _, dtype := range dataTypes {
			topic := fmt.Sprintf("market.%susdt.%s", symbol, dtype)
			// 收到数据更新时回调
			market.Subscribe(topic, func(topic string, json *huobiapi.JSON) {
				ch <- json
			})
		}
	}

	// 进入阻塞等待，这样不会导致进程退出
	market.Loop()
}
