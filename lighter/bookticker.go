package lighter

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

func (m *Manager) Subscribe(symbols []string) {
	ch := make(chan respData, 500)
	statusch := make(chan common.WsStatus, 500)
	go listeningbookticker(symbols, ch, statusch)
	go func() {
		for respdata := range ch {
			ticker, err := respdata.to_ticker(8)
			if err != nil {
				return
			}
			m.tickers[ticker.Symbol] = ticker
		}
	}()
}

func (m *Manager) SubscribeWithChan(symbols []string, channel chan<- common.Ticker, statusch chan<- common.WsStatus) {
	ch := make(chan respData, 500)
	go listeningbookticker(symbols, ch, statusch)
	go func() {
		for respdata := range ch {
			ticker, err := respdata.to_ticker(8)
			if err != nil {
				return
			}
			ticker.Exchange = "lighter"
			channel <- ticker
		}
	}()
}

func (m *Manager) GetSymbolList() []string {
	return slices.Collect(maps.Keys(m.tickers))
}

func (m *Manager) GetTicker(symbol string) common.Ticker {
	return m.tickers[symbol]
}
