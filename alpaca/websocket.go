package alpaca

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Rin18/bookticker-manager/common"
	"github.com/gorilla/websocket"
)

const baseurl = "wss://stream.data.alpaca.markets/v2/iex"
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

type authenticateMsg struct {
	Action string `json:"action"`
	Key    string `json:"key"`
	Secret string `json:"secret"`
}

type subscribeMsg struct {
	Action string   `json:"action"`
	Quotes []string `json:"quotes"`
}

type rawResponse struct {
	Type        string      `json:"T"`
	Symbol      string      `json:"S"`
	BidExchange string      `json:"bx"`
	BidPrice    json.Number `json:"bp"`
	BidAmount   json.Number `json:"bs"`
	AskExchange string      `json:"ax"`
	AskPrice    json.Number `json:"ap"`
	AskAmount   json.Number `json:"as"`
	Timestamp   time.Time   `json:"t"`
	Conditions  []string    `json:"c"`
	Tape        string      `json:"z"`
}

func NewWsClient() *WsClient {
	return &WsClient{
		exchange: "alpacaIex",
		baseurl:  baseurl,
	}
}

func (w *WsClient) Start(symbols []string, timeout time.Duration, ch chan<- common.Ticker, statusch chan<- common.WsStatus, key string, secret string) {
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
		err = w.authenticate(key, secret)
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

func (w *WsClient) authenticate(key string, secret string) error {
	authenticateMsg := authenticateMsg{
		Action: "auth",
		Key:    key,
		Secret: secret,
	}
	err := w.conn.WriteJSON(authenticateMsg)
	if err != nil {
		return err
	}
	return nil
}

func (w *WsClient) subscribe(symbols []string) error {
	subscribeMsg := subscribeMsg{Action: "subscribe", Quotes: symbols}
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
		log.Println(string(msg))
		receiveTime := time.Now().UnixMicro()
		rawresps := []rawResponse{}
		err = json.Unmarshal(msg, &rawresps)
		if err != nil {
			w.sendStatus(err.Error())
			continue
		}
		lasttickers := make(map[string]rawResponse)
		for _, rawresp := range rawresps {
			lasttickers[rawresp.Symbol] = rawresp
		}
		for _, lastticker := range lasttickers {
			if lastticker.Type != "q" {
				w.sendStatus(fmt.Sprintf("Received abnomal message: %v", lastticker))
				continue
			}
			ticker, err := lastticker.to_ticker(scale)
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
	bidprice, err := common.Parsestringtoint64(r.BidPrice.String(), scale)
	if err != nil {
		return common.Ticker{}, err
	}
	bidamount, err := common.Parsestringtoint64(r.BidAmount.String(), scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askprice, err := common.Parsestringtoint64(r.AskPrice.String(), scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askamount, err := common.Parsestringtoint64(r.AskAmount.String(), scale)
	if err != nil {
		return common.Ticker{}, err
	}
	ticker := common.Ticker{
		Symbol:          r.Symbol,
		TransactionTime: r.Timestamp.UnixMicro(),
		BidPrice:        bidprice,
		BidAmount:       bidamount,
		AskPrice:        askprice,
		AskAmount:       askamount,
	}
	return ticker, nil
}
