package binancefutures

import (
	"log"
	"testing"

	"github.com/Rin18/bookticker-manager/common"
)

func TestListeningTicker(t *testing.T) {
	ch := make(chan common.Ticker, 50)
	statusch := make(chan common.WsStatus, 50)
	listeningbookticker([]string{"btcusdt", "ethusdt", "dogeusdt"}, ch, statusch)
	for ticker := range ch {
		log.Println(ticker)
	}
}
