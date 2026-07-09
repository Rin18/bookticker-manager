package lighter

import (
	"testing"
)

func TestSubscribe(t *testing.T) {
	manager := NewManager()
	manager.Subscribe([]string{"btc", "eth", "doge"})
	select {}
}
