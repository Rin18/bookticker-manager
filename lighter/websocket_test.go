package lighter

import (
	"log"
	"testing"
	"time"

	"github.com/Rin18/bookticker-manager/common"
)

func TestListeningbookticker(t *testing.T) {
	ch := make(chan common.Ticker, 500)
	statusch := make(chan common.WsStatus, 50)
	err := listeningbookticker([]string{"btc", "eth", "doge", "sol"}, 350*time.Millisecond, ch, statusch)
	if err != nil {
		t.Errorf("error occured: %v", err)
	}
}

func TestBuildChannels(t *testing.T) {
	symbols := []string{"btc", "eth", "doge"}
	channels := buildchannels(symbols)
	want := []string{"ticker/1", "ticker/0", "ticker/3"}
	log.Println(channels)
	log.Println(want)
}
