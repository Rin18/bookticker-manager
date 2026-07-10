package binancefutures

import (
	"log"
	"testing"

	"github.com/Rin18/bookticker-manager/common"
)

func TestListeningTicker(t *testing.T) {
	ch := make(chan respData, 50)
	statusch := make(chan common.WsStatus, 50)
	listeningbookticker([]string{"btcusdt", "ethusdt", "dogeusdt"}, ch, statusch)
	for status := range statusch {
		log.Println(status)
	}
}
