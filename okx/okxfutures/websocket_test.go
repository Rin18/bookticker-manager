package okxfutures

import (
	"log"
	"testing"
	"time"

	"github.com/Rin18/bookticker-manager/common"
)

func TestStart(t *testing.T) {
	client := NewWsClient()
	symbols := []string{"btc-usdt", "eth-usdt"}
	ch := make(chan common.Ticker, 500)
	statusch := make(chan common.WsStatus, 500)
	go client.Start(symbols, 5*time.Second, ch, statusch)
	for msg := range ch {
		log.Println(msg)
	}
}
