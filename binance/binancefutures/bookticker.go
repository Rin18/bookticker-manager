package binancefutures

import (
	"maps"
	"slices"

	"github.com/Rin18/bookticker-manager/common"
)

type Manager struct {
	tickers map[string]common.Ticker
}

func NewManager() *Manager {
	return &Manager{make(map[string]common.Ticker)}
}

func (m *Manager) SubscribeWithChan(symbols []string, ch chan<- common.Ticker, statusch chan<- common.WsStatus) {
	go listeningbookticker(symbols, ch, statusch)
}

func (m *Manager) GetSymbolList() []string {
	return slices.Collect(maps.Keys(m.tickers))
}

func (m *Manager) GetTicker(symbol string) common.Ticker {
	return m.tickers[symbol]
}
