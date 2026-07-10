package binancefutures

import (
	"maps"
	"slices"
	"time"

	"github.com/Rin18/bookticker-manager/common"
)

type Manager struct {
	tickers map[string]common.Ticker
}

func NewManager() *Manager {
	return &Manager{make(map[string]common.Ticker)}
}

func (m *Manager) SubscribeWithChan(symbols []string, timeout time.Duration, ch chan<- common.Ticker, statusch chan<- common.WsStatus) {
	go listeningbookticker(symbols, timeout, ch, statusch)
}

func (m *Manager) GetSymbolList() []string {
	return slices.Collect(maps.Keys(m.tickers))
}

func (m *Manager) GetTicker(symbol string) common.Ticker {
	return m.tickers[symbol]
}
