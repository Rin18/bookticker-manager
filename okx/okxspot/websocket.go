package okxspot

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

const baseurl = "wss://ws.okx.com:8443/ws/v5/public"
const scale = 8

type WsClient struct {
	baseurl  string
	conn     *websocket.Conn
	timeout  time.Duration
	ch       chan<- common.Ticker
	statusch chan<- common.WsStatus
}

type subscribeMsg struct {
	ID        string `json:"id"`
	Operation string `json:"op"`
	Arguments []arg  `json:"args"`
}

type arg struct {
	Channel string `json:"channel"`
	InstID  string `json:"instId"`
}

type rawResponse struct {
	Arg  arg         `json:"arg"`
	Data [1]respData `json:"data"`
}

type respData struct {
	Asks       [1][4]string `json:"asks"`
	Bids       [1][4]string `json:"bids"`
	Timestamp  string       `json:"ts"`
	SequenceID int64        `json:"seqId"`
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
			statusch <- common.WsStatus{Exchange: "okxspot", Timestamp: time.Now().UnixMicro(), IsHealthy: false, ErrorMsg: fmt.Sprintf("Connect to websocket error: %v", err)}
			continue
		}
		err = w.subscribe(symbols)
		if err != nil {
			statusch <- common.WsStatus{Exchange: "okxspot", Timestamp: time.Now().UnixMicro(), IsHealthy: false, ErrorMsg: fmt.Sprintf("Subscribe websocket error: %v", err)}
			continue
		}
		statusch <- common.WsStatus{Exchange: "okxspot", Timestamp: time.Now().UnixMicro(), IsHealthy: true, ErrorMsg: ""}
		err = w.readLoop()
		if err != nil {
			statusch <- common.WsStatus{Exchange: "okxspot", Timestamp: time.Now().UnixMicro(), IsHealthy: false, ErrorMsg: fmt.Sprintf("Read Message error: %v", err)}
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
	args := []arg{}
	channel := "bbo-tbt"
	for _, symbol := range symbols {
		symbol = strings.ToUpper(symbol)
		arg := arg{Channel: channel, InstID: symbol}
		args = append(args, arg)
	}
	subscribeMsg := subscribeMsg{ID: "satoshi", Operation: "subscribe", Arguments: args}
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
		if rawresp.Data[0].Timestamp == "" {
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
	bidprice, err := common.Parsestringtoint64(r.Data[0].Bids[0][0], scale)
	if err != nil {
		return common.Ticker{}, err
	}
	bidamount, err := common.Parsestringtoint64(r.Data[0].Bids[0][1], scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askprice, err := common.Parsestringtoint64(r.Data[0].Asks[0][0], scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askamount, err := common.Parsestringtoint64(r.Data[0].Asks[0][1], scale)
	if err != nil {
		return common.Ticker{}, err
	}
	timestamp, err := strconv.ParseInt(r.Data[0].Timestamp, 10, 64)
	if err != nil {
		return common.Ticker{}, err
	}
	ticker := common.Ticker{
		Exchange:        "okxspot",
		Symbol:          r.Arg.InstID,
		TransactionTime: timestamp,
		BidPrice:        bidprice,
		BidAmount:       bidamount,
		AskPrice:        askprice,
		AskAmount:       askamount,
	}
	return ticker, nil
}
