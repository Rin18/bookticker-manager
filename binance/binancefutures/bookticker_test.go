package binancefutures

import (
	"testing"
)

func TestSubscribe(t *testing.T) {
	manager := NewManager()
	manager.Subscribe([]string{"btcusdt", "ethusdt", "dogeusdt"})
	select {}
}
