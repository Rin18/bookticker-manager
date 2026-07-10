package asterspot

import (
	"testing"
	"time"

	"github.com/Rin18/bookticker-manager/common"
)

func TestListeningbookticker(t *testing.T) {
	ch := make(chan common.Ticker, 500)
	statusch := make(chan common.WsStatus, 500)
	err := listeningbookticker([]string{"btcusdt", "ethusdt"}, 350*time.Millisecond, ch, statusch)
	if err != nil {
		t.Errorf("error occured: %v", err)
	}
}
