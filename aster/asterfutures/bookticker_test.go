package asterfutures

import (
	"log"
	"testing"

	"github.com/Rin18/bookticker-manager/common"
)

func TestSubscribeWithChannel(t *testing.T) {
	manager := NewManager()
	ch := make(chan common.Ticker, 500)
	statusch := make(chan common.WsStatus, 500)
	manager.SubscribeWithChan([]string{"btcsudt", "ethusdt", "dogeusdt"}, ch, statusch)
	for ticker := range ch {
		log.Println(ticker)
	}
}
