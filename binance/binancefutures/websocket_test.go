package binancefutures

import "testing"

func TestListeningTicker(t *testing.T) {
	ch := make(chan respData, 50)
	listeningbookticker([]string{"btcusdt", "ethusdt", "dogeusdt"}, ch)
}
