package mexcspot

import (
	"strings"
	"time"

	"github.com/Rin18/bookticker-manager/common"
	"github.com/Rin18/bookticker-manager/mexc/mexcspot/pb"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

const baseurl = "wss://wbs-api.mexc.com/ws"
const scale = 8

type WsClient struct {
	baseurl   string
	conn      *websocket.Conn
	timeout   time.Duration
	ch        chan<- common.Ticker
	statusch  chan<- common.WsStatus
	ishealthy bool
}

type subscribeMsg struct {
	Operation string   `json:"method"`
	Arguments []string `json:"params"`
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
	args := []string{}
	for _, symbol := range symbols {
		symbol = strings.ToUpper(symbol)
		arg := "spot@public.aggre.bookTicker.v3.api.pb@10ms@" + symbol
		args = append(args, arg)
	}
	subscribeMsg := subscribeMsg{Operation: "SUBSCRIPTION", Arguments: args}
	err := w.conn.WriteJSON(subscribeMsg)
	if err != nil {
		return err
	}
	return nil
}

func (w *WsClient) readLoop() error {
	for {
		w.conn.SetReadDeadline(time.Now().Add(w.timeout))
		msgType, msg, err := w.conn.ReadMessage()
		receiveTime := time.Now().UnixMicro()
		if err != nil {
			return err
		}
		switch msgType {
		case websocket.TextMessage:
		case websocket.BinaryMessage:
			rawresp := pb.PushDataV3ApiWrapper{}
			err := proto.Unmarshal(msg, &rawresp)
			if err != nil {
				w.sendStatus(err.Error())
				continue
			}
			ticker, err := get_ticker(&rawresp)
			if err != nil {
				w.sendStatus(err.Error())
				continue
			}

			if !w.ishealthy {
				w.sendStatus("")
			}
			ticker.ReceiveTime = receiveTime
			ticker.NormalizeTs()
			w.ch <- ticker
		}
	}
}

func (w *WsClient) sendStatus(errorMsg string) {
	w.ishealthy = errorMsg == ""
	statusMsg := common.WsStatus{
		Exchange:  w.baseurl,
		Timestamp: time.Now().UnixMicro(),
		ErrorMsg:  errorMsg,
		IsHealthy: w.ishealthy,
	}
	w.statusch <- statusMsg
}

func get_ticker(rawresp *pb.PushDataV3ApiWrapper) (common.Ticker, error) {
	symbol := rawresp.GetSymbol()
	pushtime := rawresp.GetSendTime()
	rawticker := rawresp.GetPublicAggreBookTicker()
	bidPrice, err := common.Parsestringtoint64(rawticker.GetBidPrice(), scale)
	if err != nil {
		return common.Ticker{}, err
	}
	bidAmount, err := common.Parsestringtoint64(rawticker.GetBidQuantity(), scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askPrice, err := common.Parsestringtoint64(rawticker.GetAskPrice(), scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askAmount, err := common.Parsestringtoint64(rawticker.GetAskQuantity(), scale)
	if err != nil {
		return common.Ticker{}, err
	}
	transactionTime := rawticker.GetLastOrderCreateTime()
	ticker := common.Ticker{
		Exchange:        "mexcspot",
		Symbol:          symbol,
		PushTime:        pushtime,
		TransactionTime: transactionTime,
		BidPrice:        bidPrice,
		BidAmount:       bidAmount,
		AskPrice:        askPrice,
		AskAmount:       askAmount,
	}
	return ticker, nil
}
