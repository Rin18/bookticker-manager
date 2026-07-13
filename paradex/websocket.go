package paradex

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Rin18/bookticker-manager/common"
	"github.com/gorilla/websocket"
)

const baseurl = "wss://ws.api.prod.paradex.trade/v1"
const scale = 8

type WsClient struct {
	baseurl  string
	conn     *websocket.Conn
	timeout  time.Duration
	ch       chan<- common.Ticker
	statusch chan<- common.WsStatus
}

type subscribeMsg struct {
	JsonRPC string            `json:"jsonrpc"`
	Method  string            `json:"method"`
	Params  map[string]string `json:"params"`
	ID      int               `json:"id"`
}

type rawResponse struct {
	JsonRPC string     `json:"jsonrpc"`
	Method  string     `json:"method"`
	Params  RespParams `json:"params"`
}

type RespParams struct {
	Channel string   `json:"channel"`
	Data    RespData `json:"data"`
}

type RespData struct {
	Symbol          string `json:"market"`
	SeqNo           int64  `json:"seq_no"`
	AskPrice        string `json:"ask"`
	AskAmount       string `json:"ask_size"`
	BidPrice        string `json:"bid"`
	BidAmount       string `json:"bid_size"`
	TransactionTime int64  `json:"last_updated_at"`
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
			statusch <- common.WsStatus{Exchange: "paradex", Timestamp: time.Now().UnixMicro(), IsHealthy: false, ErrorMsg: fmt.Sprintf("Connect to websocket error: %v", err)}
			continue
		}
		err = w.subscribe(symbols)
		if err != nil {
			statusch <- common.WsStatus{Exchange: "paradex", Timestamp: time.Now().UnixMicro(), IsHealthy: false, ErrorMsg: fmt.Sprintf("Subscribe websocket error: %v", err)}
			continue
		}
		statusch <- common.WsStatus{Exchange: "paradex", Timestamp: time.Now().UnixMicro(), IsHealthy: true, ErrorMsg: ""}
		err = w.readLoop()
		if err != nil {
			statusch <- common.WsStatus{Exchange: "paradex", Timestamp: time.Now().UnixMicro(), IsHealthy: false, ErrorMsg: fmt.Sprintf("Read Message error: %v", err)}
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
	for _, symbol := range symbols {
		symbol = strings.ToUpper(symbol)
		param := "bbo." + symbol
		subscribeMsg := subscribeMsg{JsonRPC: "2.0", Method: "subscribe", Params: map[string]string{"channel": param}, ID: 0}
		err := w.conn.WriteJSON(subscribeMsg)
		if err != nil {
			return err
		}
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
		if rawresp.Params.Data.Symbol == "" {
			continue
		}
		ticker, err := rawresp.to_ticker(scale)
		if err != nil {
			slog.Error("websocket read loop error", "to ticker error: ", err, "exchange: ", w.baseurl)
			continue
		}
		ticker.ReceiveTime = receiveTime
		w.ch <- ticker
	}
}

func (r *rawResponse) to_ticker(scale int) (common.Ticker, error) {
	bidprice, err := common.Parsestringtoint64(r.Params.Data.BidPrice, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	bidamount, err := common.Parsestringtoint64(r.Params.Data.BidAmount, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askprice, err := common.Parsestringtoint64(r.Params.Data.AskPrice, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askamount, err := common.Parsestringtoint64(r.Params.Data.AskAmount, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	ticker := common.Ticker{
		Exchange:        "paradex",
		Symbol:          r.Params.Data.Symbol,
		TransactionTime: r.Params.Data.TransactionTime,
		BidPrice:        bidprice,
		BidAmount:       bidamount,
		AskPrice:        askprice,
		AskAmount:       askamount,
	}
	return ticker, nil
}
