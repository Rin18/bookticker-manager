package binancefutures

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/Rin18/bookticker-manager/common"
	"github.com/gorilla/websocket"
)

type respData struct {
	EventType       string `json:"e"`
	UpdateId        int64  `json:"u"`
	Symbol          string `json:"s"`
	PushTime        int64  `json:"E"`
	TransactionTime int64  `json:"T"`
	BidPrice        string `json:"b"`
	BidAmount       string `json:"B"`
	AskPrice        string `json:"a"`
	AskAmount       string `json:"A"`
}

func listeningbookticker(symbols []string, ch chan<- respData) error {
	params := buildparamshelper(symbols)
	u := "wss://fstream.binance.com/public/ws/" + params
	retrycount := 0
	sleepduration := []time.Duration{0 * time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 15 * time.Second}
	lastretry := time.Now()
	for {
		if time.Since(lastretry) > 120*time.Second {
			retrycount = 0
		}
		if retrycount >= len(sleepduration) {
			time.Sleep(sleepduration[len(sleepduration)-1])
		} else {
			time.Sleep(sleepduration[retrycount])
		}
		lastretry = time.Now()
		retrycount++
		c, _, err := websocket.DefaultDialer.Dial(u, nil)
		if err != nil {
			continue
		}
		const pingwait = 200 * time.Second
		c.SetReadDeadline(time.Now().Add(pingwait))
		c.SetPingHandler(func(appData string) error {
			c.SetReadDeadline(time.Now().Add(pingwait))
			return c.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
		})
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				break
			}
			respdata := respData{}
			err = json.Unmarshal(msg, &respdata)
			if err != nil {
				break
			}
			ch <- respdata
		}
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
	ticker := common.Ticker{Symbol: r.Symbol, PushTime: r.PushTime, TransactionTime: r.TransactionTime, BidPrice: bidprice, BidAmount: bidamount, AskPrice: askprice, AskAmount: askamount}
	return ticker, nil
}
