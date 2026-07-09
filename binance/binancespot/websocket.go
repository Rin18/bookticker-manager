package binancespot

import (
	"encoding/json"
	"strings"

	"github.com/Rin18/bookticker-manager/common"
	"github.com/gorilla/websocket"
)

type respData struct {
	UpdateId  int64  `json:"u"`
	Symbol    string `json:"s"`
	BidPrice  string `json:"b"`
	BidAmount string `json:"B"`
	AskPrice  string `json:"a"`
	AskAmount string `json:"A"`
}

func listeningbookticker(symbols []string, ch chan<- respData) error {
	params := buildparamshelper(symbols)
	u := "wss://stream.binance.com:9443/ws/" + params
	c, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	for {
		_, msg, err := c.ReadMessage()
		if err != nil {
			return err
		}
		respdata := respData{}
		err = json.Unmarshal(msg, &respdata)
		if err != nil {
			return err
		}
		ch <- respdata
	}
}

func buildparamshelper(symbols []string) string {
	params := []string{}
	for _, symbol := range symbols {
		lowercasedSymbol := strings.ToLower(symbol)
		streamName := lowercasedSymbol + "@bookTicker"
		params = append(params, streamName)
	}
	return strings.Join(params, "/")
}

func (r *respData) to_ticker(scale int) (Ticker, error) {
	bidprice, err := common.Parsestringtoint64(r.BidPrice, scale)
	if err != nil {
		return Ticker{}, err
	}
	bidamount, err := common.Parsestringtoint64(r.BidAmount, scale)
	if err != nil {
		return Ticker{}, err
	}
	askprice, err := common.Parsestringtoint64(r.AskPrice, scale)
	if err != nil {
		return Ticker{}, err
	}
	askamount, err := common.Parsestringtoint64(r.AskAmount, scale)
	if err != nil {
		return Ticker{}, err
	}
	ticker := Ticker{symbol: r.Symbol, updateid: r.UpdateId, BidPrice: bidprice, BidAmount: bidamount, AskPrice: askprice, AskAmount: askamount}
	return ticker, nil
}
