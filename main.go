package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/ftao/ssmdb/exchanges/binance"
	"github.com/ftao/ssmdb/exchanges/common"
	"github.com/ftao/ssmdb/exchanges/huobipro"
	"github.com/ftao/ssmdb/exchanges/okex"
	"github.com/ftao/ssmdb/storage"
)

var exchangeName = flag.String("exchange", "huobipro", "exchange name")

func makeExchange(name string) common.IExchange {
	switch name {
	case "huobipro":
		return huobipro.NewExchange(name)
	case "okex":
		return okex.NewExchange(name)
	case "binance":
		return binance.NewExchange(name)
	default:
		panic("invalid exchange name")
	}
}

func publishVars() {
	http.ListenAndServe(":8000", nil)
}

func main() {
	flag.Parse()
	saveDir := "data4/" + *exchangeName

	// 创建客户端实例
	exchange := makeExchange(*exchangeName)
	log.Printf("fetch data from exchange %s, save to %s", *exchangeName, saveDir)

	msgCh := make(chan common.Message, 100)
	// read message from market
	go func() {
		for {
			msgs, err := exchange.Recv()
			if err != nil {
				log.Printf("recv error: %s", err)
				continue
			}
			for _, msg := range msgs {
				msgCh <- msg
			}
		}
	}()

	done := make(chan int, 1)
	go func() {
		store := storage.NewFsStore(saveDir)
		for msg := range msgCh {
			err := store.Insert(msg.Topic, msg.Data)
			if err != nil {
				panic(err)
			}
		}
		store.Close()
		done <- 1
	}()

	go publishVars()

	// write exit signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		log.Printf("recv signal %s", sig)
		close(msgCh)
	}()
	<-done
}
