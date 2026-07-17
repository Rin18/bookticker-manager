package binancespot

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Rin18/bookticker-manager/common"
	"github.com/gorilla/websocket"
)

const baseurl = "wss://stream.binance.com:9443/stream"
const scale = 8

type WsClient struct {
	exchange  string
	baseurl   string
	conn      *websocket.Conn
	timeout   time.Duration
	ch        chan<- common.Ticker
	statusch  chan<- common.WsStatus
	ishealthy bool
}

type subscribeMsg struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
	ID     string   `json:"id"`
}

type rawResponse struct {
	Stream string   `json:"stream"`
	Data   respData `json:"data"`
}

type respData struct {
	UpdateID  int64  `json:"u"`
	Symbol    string `json:"s"`
	BidPrice  string `json:"b"`
	BidAmount string `json:"B"`
	AskPrice  string `json:"a"`
	AskAmount string `json:"A"`
}

func NewWsClient() *WsClient {
	return &WsClient{
		exchange: "binancespot",
		baseurl:  baseurl,
	}
}

func (w *WsClient) Start(symbols []string, timeout time.Duration, ch chan<- common.Ticker, statusch chan<- common.WsStatus) {
	w.timeout = timeout
	w.ch = ch
	w.statusch = statusch
	retrycount := 0
	sleepduration := []time.Duration{0 * time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 15 * time.Second}
	lastconnect := time.Now()
	for {
		if time.Since(lastconnect) > 120*time.Second {
			retrycount = 0
		}
		if retrycount >= len(sleepduration) {
			time.Sleep(sleepduration[len(sleepduration)-1])
		} else {
			time.Sleep(sleepduration[retrycount])
		}
		lastconnect = time.Now()
		retrycount++

		err := w.connect()
		if err != nil {
			w.sendStatus(err.Error())
			continue
		}
		err = w.subscribe(symbols)
		if err != nil {
			w.sendStatus(err.Error())
			continue
		}
		err = w.readLoop()
		if err != nil {
			w.sendStatus(err.Error())
			continue
		}
	}
}

func (w *WsClient) connect() error {
	c, _, err := websocket.DefaultDialer.Dial(w.baseurl, nil)
	if err != nil {
		return err
	}
	w.conn = c
	return nil
}

func (w *WsClient) subscribe(symbols []string) error {
	params := []string{}
	for _, symbol := range symbols {
		symbol = strings.ToLower(symbol)
		param := symbol + "@bookTicker"
		params = append(params, param)
	}
	subscribeMsg := subscribeMsg{Method: "SUBSCRIBE", Params: params, ID: "123456"}
	err := w.conn.WriteJSON(subscribeMsg)
	if err != nil {
		return err
	}
	return nil
}

func (w *WsClient) readLoop() error {
	for {
		w.conn.SetReadDeadline(time.Now().Add(w.timeout))
		_, msg, err := w.conn.ReadMessage()
		if err != nil {
			return err
		}
		receiveTime := time.Now().UnixMicro()
		rawresp := rawResponse{}
		err = json.Unmarshal(msg, &rawresp)
		if err != nil {
			w.sendStatus(err.Error())
			continue
		}
		if rawresp.Data.BidPrice == "" {
			w.sendStatus(fmt.Sprintf("Received abnomal message: %v", rawresp))
			continue
		}
		ticker, err := rawresp.to_ticker(scale)
		if err != nil {
			w.sendStatus(err.Error())
			continue
		}
		if !w.ishealthy {
			w.sendStatus("")
		}
		ticker.Exchange = w.exchange
		ticker.ReceiveTime = receiveTime
		ticker.NormalizeTs()
		w.ch <- ticker
	}
}

func (w *WsClient) sendStatus(errorMsg string) {
	w.ishealthy = errorMsg == ""
	statusMsg := common.WsStatus{
		Exchange:  w.exchange,
		Timestamp: time.Now().UnixMicro(),
		ErrorMsg:  errorMsg,
		IsHealthy: w.ishealthy,
	}
	w.statusch <- statusMsg
}

func (r *rawResponse) to_ticker(scale int) (common.Ticker, error) {
	bidprice, err := common.Parsestringtoint64(r.Data.BidPrice, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	bidamount, err := common.Parsestringtoint64(r.Data.BidAmount, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askprice, err := common.Parsestringtoint64(r.Data.AskPrice, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askamount, err := common.Parsestringtoint64(r.Data.AskAmount, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	ticker := common.Ticker{
		Symbol:    r.Data.Symbol,
		BidPrice:  bidprice,
		BidAmount: bidamount,
		AskPrice:  askprice,
		AskAmount: askamount,
	}
	return ticker, nil
}
