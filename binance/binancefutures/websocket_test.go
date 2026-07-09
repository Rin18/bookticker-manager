package binancefutures

import "testing"

func TestListeningTicker(t *testing.T) {
	ch := make(chan RespData, 50)
	Listeningbookticker([]string{"btcusdt", "ethusdt", "dogeusdt"}, ch)
}
