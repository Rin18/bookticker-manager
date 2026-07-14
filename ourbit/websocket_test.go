package ourbit

import (
	"log/slog"
	"testing"
	"time"

	"github.com/Rin18/bookticker-manager/common"
)

func TestStart(t *testing.T) {
	client := NewWsClient()
	ch := make(chan common.Ticker, 500)
	statusch := make(chan common.WsStatus, 500)
	symbols := []string{"ETHUSDT"}
	go client.Start(symbols, 5*time.Second, ch, statusch)
	go func() {
		for statusmsg := range statusch {
			slog.Info("msg", "statusmsg: ", statusmsg)
		}
	}()
	for msg := range ch {
		slog.Info("msg", "msg: ", msg)
	}
}
