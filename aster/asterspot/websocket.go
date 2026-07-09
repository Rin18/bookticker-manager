package asterspot

import (
	"encoding/json"

	"github.com/Rin18/bookticker-manager/common"
	"github.com/gorilla/websocket"
)

type RespData struct {
	UpdateId  int64  `json:"u"`
	Symbol    string `json:"s"`
	BidPrice  string `json:"b"`
	BidAmount string `json:"B"`
	AskPrice  string `json:"a"`
	AskAmount string `json:"A"`
}

func Listeningbookticker(ch chan<- RespData) error {
	params := "!bookTicker"
	u := "wss://sstream.asterdex.com/ws/" + params
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
		respdata := RespData{}
		err = json.Unmarshal(msg, &respdata)
		if err != nil {
			return err
		}
		ch <- respdata
	}
}

func (r *RespData) to_ticker(scale int) (Ticker, error) {
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
