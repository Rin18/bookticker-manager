package nado

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/Rin18/bookticker-manager/common"
	"github.com/gorilla/websocket"
)

const baseurl = "wss://direct-gateway.prod.nado-backend.xyz/v1/subscribe"
const scale = 8

type WsClient struct {
	baseurl  string
	conn     *websocket.Conn
	timeout  time.Duration
	ch       chan<- common.Ticker
	statusch chan<- common.WsStatus
}

type subscribeMsg struct {
	Method string `json:"method"`
	Stream stream `json:"stream"`
	ID     int64  `json:"id"`
}

type stream struct {
	Type      string `json:"type"`
	ProductID int64  `json:"product_id"`
}

type rawResponse struct {
	Type            string `json:"type"`
	TransactionTime string `json:"timestamp"`
	ProductID       int64  `json:"product_id"`
	BidPrice        string `json:"bid_price"`
	BidAmount       string `json:"bid_qty"`
	AskPrice        string `json:"ask_price"`
	AskAmount       string `json:"ask_qty"`
}

func NewWsClient() *WsClient {
	return &WsClient{
		baseurl: baseurl,
	}
}

func (w *WsClient) Start(symbols []int64, timeout time.Duration, ch chan<- common.Ticker, statusch chan<- common.WsStatus) {
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
			statusch <- common.WsStatus{Exchange: "nado", Timestamp: time.Now().UnixMicro(), IsHealthy: false, ErrorMsg: fmt.Sprintf("Connect to websocket error: %v", err)}
			continue
		}
		err = w.subscribe(symbols)
		if err != nil {
			statusch <- common.WsStatus{Exchange: "nado", Timestamp: time.Now().UnixMicro(), IsHealthy: false, ErrorMsg: fmt.Sprintf("Subscribe websocket error: %v", err)}
			continue
		}
		statusch <- common.WsStatus{Exchange: "nado", Timestamp: time.Now().UnixMicro(), IsHealthy: true, ErrorMsg: ""}
		err = w.readLoop()
		if err != nil {
			statusch <- common.WsStatus{Exchange: "nado", Timestamp: time.Now().UnixMicro(), IsHealthy: false, ErrorMsg: fmt.Sprintf("Read Message error: %v", err)}
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

func (w *WsClient) subscribe(symbols []int64) error {
	for _, symbol := range symbols {
		streamMsg := stream{Type: "best_bid_offer", ProductID: symbol}
		subscribeMsg := subscribeMsg{Method: "subscribe", Stream: streamMsg, ID: 0}
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
		if rawresp.BidPrice == "" {
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
	bidprice, err := parse1e18stringtoint64(r.BidPrice, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	bidamount, err := parse1e18stringtoint64(r.BidAmount, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askprice, err := parse1e18stringtoint64(r.AskPrice, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askamount, err := parse1e18stringtoint64(r.AskAmount, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	transactionTime, err := strconv.ParseInt(r.TransactionTime, 10, 64)
	if err != nil {
		return common.Ticker{}, err
	}
	ticker := common.Ticker{
		Exchange:        "nado",
		Symbol:          strconv.FormatInt(r.ProductID, 10),
		TransactionTime: transactionTime,
		BidPrice:        bidprice,
		BidAmount:       bidamount,
		AskPrice:        askprice,
		AskAmount:       askamount,
	}
	return ticker, nil
}

func parse1e18stringtoint64(s string, scale int) (int64, error) {
	dropzeros := 18 - scale
	n := len(s)
	if n < dropzeros {
		return 0, fmt.Errorf("string %v is less than 10 digits", s)
	}
	expectedZeros := strings.Repeat("0", dropzeros)
	if s[n-dropzeros:] != expectedZeros {
		return 0, fmt.Errorf("Found non-zero values in the last %d digits: %v", dropzeros, s)
	}
	if n == dropzeros {
		return 0, nil
	}
	return strconv.ParseInt(s[:n-dropzeros], 10, 64)
}
