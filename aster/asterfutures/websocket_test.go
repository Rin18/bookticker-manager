package asterfutures

import (
	"testing"

	"github.com/Rin18/bookticker-manager/common"
)

func TestListeningbookticker(t *testing.T) {
	ch := make(chan common.Ticker, 500)
	statusch := make(chan common.WsStatus, 500)
	err := listeningbookticker([]string{"btcusdt", "ethusdt"}, ch, statusch)
	if err != nil {
		t.Errorf("error occured: %v", err)
	}
}
