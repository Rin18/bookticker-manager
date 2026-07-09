package lighter

import (
	"log"
	"maps"
	"slices"
)

type Manager struct {
	tickers map[string]Ticker
}

type Ticker struct {
	symbol    string
	updateid  int64
	BidPrice  int64
	BidAmount int64
	AskPrice  int64
	AskAmount int64
}

func NewManager() *Manager {
	return &Manager{make(map[string]Ticker)}
}

func (m *Manager) Subscribe(symbols []string) {
	ch := make(chan respData, 500)
	go listeningbookticker(symbols, ch)
	go func() {
		for respdata := range ch {
			ticker, err := respdata.to_ticker(8)
			if err != nil {
				return
			}
			m.tickers[ticker.symbol] = ticker
			log.Println(ticker)
		}
	}()
}

func (m *Manager) GetSymbolList() []string {
	return slices.Collect(maps.Keys(m.tickers))
}

func (m *Manager) GetTicker(symbol string) Ticker {
	return m.tickers[symbol]
}
