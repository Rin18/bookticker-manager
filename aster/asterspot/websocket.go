package asterspot

import (
	"encoding/json"
	"strings"

	"github.com/Rin18/bookticker-manager/common"
	"github.com/gorilla/websocket"
)

type rawRespData struct {
	Stream string   `json:"stream"`
	Data   respData `json:"data"`
}

type respData struct {
	UpdateId  int64  `json:"u"`
	Symbol    string `json:"s"`
	BidPrice  string `json:"b"`
	BidAmount string `json:"B"`
	AskPrice  string `json:"a"`
	AskAmount string `json:"A"`
}

func listeningbookticker(symbols []string, ch chan<- respData) error {
	params := buildchannels(symbols)
	u := "wss://sstream.asterdex.com/stream?streams=" + params
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
		respdata := rawRespData{}
		err = json.Unmarshal(msg, &respdata)
		if err != nil {
			return err
		}
		ch <- respdata.Data
	}
}

func (r *respData) to_ticker(scale int) (common.Ticker, error) {
	bidprice, err := common.Parsestringtoint64(r.BidPrice, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	bidamount, err := common.Parsestringtoint64(r.BidAmount, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askprice, err := common.Parsestringtoint64(r.AskPrice, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askamount, err := common.Parsestringtoint64(r.AskAmount, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	ticker := common.Ticker{Symbol: r.Symbol, UpdateId: r.UpdateId, BidPrice: bidprice, BidAmount: bidamount, AskPrice: askprice, AskAmount: askamount}
	return ticker, nil
}

func buildchannels(symbols []string) string {
	channels := []string{}
	for _, symbol := range symbols {
		symbol = strings.ToLower(symbol)
		channel := symbol + "@bookTicker"
		channels = append(channels, channel)
	}
	return strings.Join(channels, "/")
}
