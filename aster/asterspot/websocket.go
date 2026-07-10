package asterspot

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/Rin18/bookticker-manager/common"
	"github.com/gorilla/websocket"
)

type rawRespData struct {
	Stream string   `json:"stream"`
	Data   respData `json:"data"`
}

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

func listeningbookticker(symbols []string, ch chan<- common.Ticker, statusch chan<- common.WsStatus) error {
	params := buildchannels(symbols)
	u := "wss://sstream.asterdex.com/stream?streams=" + params
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
			statusch <- common.WsStatus{Exchange: "asterspot", IsHealthy: false, ErrorMsg: "websocket dial failed: " + err.Error()}
			continue
		}

		const pingwait = 190 * time.Second
		c.SetReadDeadline(time.Now().Add(pingwait))
		c.SetPingHandler(func(appData string) error {
			c.SetReadDeadline(time.Now().Add(pingwait))
			return c.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
		})
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				statusch <- common.WsStatus{Exchange: "asterspot", IsHealthy: false, ErrorMsg: "websocket read failed: " + err.Error()}
				break
			}
			respdata := rawRespData{}
			err = json.Unmarshal(msg, &respdata)
			if err != nil {
				statusch <- common.WsStatus{Exchange: "asterspot", IsHealthy: false, ErrorMsg: "websocket unmarshal failed: " + err.Error()}
				break
			}
			ticker, err := respdata.Data.to_ticker("asterspot", 8)
			if err != nil {
				statusch <- common.WsStatus{Exchange: "asterspot", IsHealthy: false, ErrorMsg: "respdata to_ticker failed: " + err.Error()}
				continue
			}
			if !hasSentHealthy {
				statusch <- common.WsStatus{Exchange: "asterspot", IsHealthy: true}
				hasSentHealthy = true
			}
			ch <- ticker
		}
	}
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
	ticker := common.Ticker{Exchange: exchange, Symbol: r.Symbol, PushTime: r.PushTime, TransactionTime: r.TransactionTime, BidPrice: bidprice, BidAmount: bidamount, AskPrice: askprice, AskAmount: askamount}
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
