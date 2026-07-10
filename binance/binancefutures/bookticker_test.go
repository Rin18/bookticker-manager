package binancefutures

import (
	"log"
	"testing"
	"time"

	"github.com/Rin18/bookticker-manager/common"
)

func TestSubscribeWithChannel(t *testing.T) {
	manager := NewManager()
	ch := make(chan common.Ticker, 500)
	statusch := make(chan common.WsStatus, 500)
	manager.SubscribeWithChan([]string{"btcusdt", "ethusdt", "dogeusdt"}, 350*time.Millisecond, ch, statusch)
	for ticker := range ch {
		log.Println(ticker)
	}
}
