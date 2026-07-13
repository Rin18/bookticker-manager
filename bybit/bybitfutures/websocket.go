package bybitfutures

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Rin18/bookticker-manager/common"
	"github.com/gorilla/websocket"
)

const baseurl = "wss://stream.bybit.com/v5/public/linear"
const scale = 8

type WsClient struct {
	baseurl  string
	conn     *websocket.Conn
	timeout  time.Duration
	ch       chan<- common.Ticker
	statusch chan<- common.WsStatus
}

type subscribeMsg struct {
	Operation string   `json:"op"`
	Arguments []string `json:"args"`
}

type rawResponse struct {
	Topic           string   `json:"topic"`
	Type            string   `json:"type"`
	PushTime        int64    `json:"ts"`
	Data            respData `json:"data"`
	TransactionTime int64    `json:"cts"`
}

type respData struct {
	Symbol        string       `json:"s"`
	Bids          [1][2]string `json:"b"`
	Asks          [1][2]string `json:"a"`
	UpdateID      int64        `json:"u"`
	CrossSequence int64        `json:"seq"`
}

func NewWsClient() *WsClient {
	return &WsClient{
		baseurl: baseurl,
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
			statusch <- common.WsStatus{Exchange: "bybitfutures", Timestamp: time.Now().UnixMicro(), IsHealthy: false, ErrorMsg: fmt.Sprintf("Connect to websocket error: %v", err)}
			continue
		}
		err = w.subscribe(symbols)
		if err != nil {
			statusch <- common.WsStatus{Exchange: "bybitfutures", Timestamp: time.Now().UnixMicro(), IsHealthy: false, ErrorMsg: fmt.Sprintf("Subscribe websocket error: %v", err)}
			continue
		}
		statusch <- common.WsStatus{Exchange: "bybitfutures", Timestamp: time.Now().UnixMicro(), IsHealthy: true, ErrorMsg: ""}
		err = w.readLoop()
		if err != nil {
			statusch <- common.WsStatus{Exchange: "bybitfutures", Timestamp: time.Now().UnixMicro(), IsHealthy: false, ErrorMsg: fmt.Sprintf("Read Message error: %v", err)}
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
	args := []string{}
	for _, symbol := range symbols {
		symbol = strings.ToUpper(symbol)
		arg := "orderbook.1." + symbol
		args = append(args, arg)
	}
	subscribeMsg := subscribeMsg{Operation: "subscribe", Arguments: args}
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
			slog.Error("websocket read loop error", "json unmarshal error: ", err, "exchange: ", w.baseurl)
			continue
		}
		if rawresp.Data.Symbol == "" {
			continue
		}
		ticker, err := rawresp.to_ticker(scale)
		if err != nil {
			slog.Error("websocket read loop error", "to ticker error: ", err, "exchange: ", w.baseurl)
			continue
		}
		ticker.ReceiveTime = receiveTime
		ticker.NormalizeTs()
		w.ch <- ticker
	}
}

func (r *rawResponse) to_ticker(scale int) (common.Ticker, error) {
	bidprice, err := common.Parsestringtoint64(r.Data.Bids[0][0], scale)
	if err != nil {
		return common.Ticker{}, err
	}
	bidamount, err := common.Parsestringtoint64(r.Data.Bids[0][1], scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askprice, err := common.Parsestringtoint64(r.Data.Asks[0][0], scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askamount, err := common.Parsestringtoint64(r.Data.Asks[0][1], scale)
	if err != nil {
		return common.Ticker{}, err
	}
	ticker := common.Ticker{
		Exchange:        "bybitfutures",
		Symbol:          r.Data.Symbol,
		PushTime:        r.PushTime,
		TransactionTime: r.TransactionTime,
		BidPrice:        bidprice,
		BidAmount:       bidamount,
		AskPrice:        askprice,
		AskAmount:       askamount,
	}
	return ticker, nil
}
