package okxfutures

import (
	"log"
	"strings"

	"github.com/Rin18/bookticker-manager/common"
	"github.com/gorilla/websocket"
)

const baseurl = "wss://ws.okx.com:8443/ws/v5/public"

type WsClient struct {
	baseurl string
	conn    *websocket.Conn
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

func NewWsClient() *WsClient {
	return &WsClient{
		baseurl: baseurl,
	}
}

func (w *WsClient) Start(symbols []string, ch chan<- common.Ticker, statusch chan<- common.WsStatus) {
	for {
		err := w.connect()
		if err != nil {
			continue
		}
		err = w.subscribe(symbols)
		if err != nil {
			continue
		}
		w.readLoop(ch, statusch)
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

func (w *WsClient) readLoop(ch chan<- common.Ticker, statusch chan<- common.WsStatus) {
	for {
		_, msg, err := w.conn.ReadMessage()
		if err != nil {
			// pending
			continue
		}
		log.Println(string(msg))
	}
}
