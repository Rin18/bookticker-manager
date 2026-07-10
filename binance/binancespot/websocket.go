package binancespot

import (
	"encoding/json"
	"strings"
	"time"

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

func listeningbookticker(symbols []string, timeout time.Duration, ch chan<- common.Ticker, statusch chan<- common.WsStatus) error {
	params := buildparamshelper(symbols)
	u := "wss://stream.binance.com:9443/ws/" + params
	retrycount := 0
	sleepduration := []time.Duration{0 * time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 15 * time.Second}
	lastretry := time.Now()
	for {
		hasSentHealthy := false
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
			statusch <- common.WsStatus{Exchange: "binancespot", IsHealthy: false, ErrorMsg: "websocket dial failed: " + err.Error()}
			continue
		}

		for {
			c.SetReadDeadline(time.Now().Add(timeout))
			_, msg, err := c.ReadMessage()
			if err != nil {
				statusch <- common.WsStatus{Exchange: "binancespot", IsHealthy: false, ErrorMsg: "websocket read failed: " + err.Error()}
				break
			}
			respdata := respData{}
			err = json.Unmarshal(msg, &respdata)
			if err != nil {
				statusch <- common.WsStatus{Exchange: "binancespot", IsHealthy: false, ErrorMsg: "websocket unmarshal failed: " + err.Error()}
				break
			}
			ticker, err := respdata.to_ticker("binancespot", 8)
			if err != nil {
				statusch <- common.WsStatus{Exchange: "binancespot", IsHealthy: false, ErrorMsg: "respdata to_ticker failed: " + err.Error()}
				continue
			}
			if !hasSentHealthy {
				statusch <- common.WsStatus{Exchange: "binancespot", IsHealthy: true}
				hasSentHealthy = true
			}
			ch <- ticker
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

func (r *respData) to_ticker(exchange string, scale int) (common.Ticker, error) {
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
	ticker := common.Ticker{Exchange: exchange, Symbol: r.Symbol, BidPrice: bidprice, BidAmount: bidamount, AskPrice: askprice, AskAmount: askamount}
	return ticker, nil
}
