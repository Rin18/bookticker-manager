package bybitfutures

import (
	"log"
	"log/slog"
	"testing"
	"time"

	"github.com/Rin18/bookticker-manager/common"
)

func TestStart(t *testing.T) {
	client := NewWsClient()
	ch := make(chan common.Ticker, 500)
	statusch := make(chan common.WsStatus, 500)
	symbols := []string{"btcusdt", "ethusdt"}
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

func TestSubscribe(t *testing.T) {
	client := NewWsClient()
	err := client.connect()
	if err != nil {
		log.Println(err)
	}
	err = client.subscribe([]string{"btcusdt", "ethusdt"})
	if err != nil {
		log.Println(err)
	}
	client.timeout = 5 * time.Second
	err = client.readLoop()
	if err != nil {
		log.Println(err)
	}
}
