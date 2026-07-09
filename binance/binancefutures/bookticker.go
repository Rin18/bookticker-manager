package binancefutures

import (
	"maps"
	"slices"
)

type Manager struct {
	tickers map[string]Ticker
}

type Ticker struct {
	symbol    string
	updateid  int64
	bidprice  int64
	bidamount int64
	askprice  int64
	askamount int64
}

func NewManager() *Manager {
	return &Manager{make(map[string]Ticker)}
}

func (m *Manager) Subscribe(symbols []string) {
	ch := make(chan RespData, 500)
	go Listeningbookticker(symbols, ch)
	go func() {
		for respdata := range ch {
			ticker, err := respdata.to_ticker(8)
			if err != nil {
				return
			}
			m.tickers[ticker.symbol] = ticker
		}
	}()
}

func (m *Manager) GetSymbolList() []string {
	return slices.Collect(maps.Keys(m.tickers))
}

func (m *Manager) GetTicker(symbol string) Ticker {
	return m.tickers[symbol]
}
