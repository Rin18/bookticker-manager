package lighter

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
	manager.SubscribeWithChan([]string{"btc", "eth", "doge"}, 350*time.Millisecond, ch, statusch)
	go func() {
		for ticker := range ch {
			log.Println(ticker)
		}
	}()
	for status := range statusch {
		log.Println(status)
	}
}
