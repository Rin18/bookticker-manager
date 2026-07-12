package okxfutures

import (
	"testing"

	"github.com/Rin18/bookticker-manager/common"
)

func TestStart(t *testing.T) {
	client := NewWsClient()
	symbols := []string{"btc-usdt", "eth-usdt"}
	ch := make(chan common.Ticker, 500)
	statusch := make(chan common.WsStatus, 500)
	client.Start(symbols, ch, statusch)
}
