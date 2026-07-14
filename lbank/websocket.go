package lbank

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Rin18/bookticker-manager/common"
	"github.com/gorilla/websocket"
)

const (
	baseurl = "wss://uuws.rerrkvifj.com/ws/v3"
	scale   = 8

	// 未配置的 symbol 默认使用 0.1。
	defaultDepthDecimal = "0.1"

	// 实测只有 limit=25 会持续返回 z=4。
	depthLimit = 25
)

type WsClient struct {
	baseurl  string
	conn     *websocket.Conn
	timeout  time.Duration
	ch       chan<- common.Ticker
	statusch chan<- common.WsStatus

	writeMu sync.Mutex

	// LBank Depth 回包没有 symbol，只返回订阅流水号 y，
	// 所以需要用 y 找回对应的 symbol。
	symbolByTSN map[string]string

	// 每个合约的价格聚合精度。
	decimalBySymbol map[string]string
}

type subscribeMsg struct {
	Ext       string         `json:"e"`
	Topic     int            `json:"x"`
	TSN       string         `json:"y"`
	Parameter subscribeParam `json:"a"`
	Type      int            `json:"z"`
}

type subscribeParam struct {
	InstrumentID string `json:"i"`
}

type heartbeatMsg struct {
	Action string `json:"action"`
	Ping   string `json:"ping"`
}

type rawResponse struct {
	Ext      string      `json:"e"`
	Bids     [][2]string `json:"b"`
	Asks     [][2]string `json:"s"`
	PushTime int64       `json:"w"`
	Topic    int         `json:"x"`
	TSN      string      `json:"y"`
	Type     int         `json:"z"`
}

func NewWsClient() *WsClient {
	return &WsClient{
		baseurl:         baseurl,
		symbolByTSN:     make(map[string]string),
		decimalBySymbol: make(map[string]string),
	}
}

// SetDepthDecimal 手工配置合约的价格聚合精度。
//
// 必须在 Start 前调用。例如：
//
//	client.SetDepthDecimal("BTCUSDT", "0.1")
//	client.SetDepthDecimal("ETHUSDT", "0.01")
//
// 如果没有配置，默认使用 0.1。
func (w *WsClient) SetDepthDecimal(symbol, decimal string) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	decimal = strings.TrimSpace(decimal)

	if symbol == "" || decimal == "" {
		return
	}

	w.decimalBySymbol[symbol] = decimal
}

func (w *WsClient) Start(
	symbols []string,
	timeout time.Duration,
	ch chan<- common.Ticker,
	statusch chan<- common.WsStatus,
) {
	w.timeout = timeout
	w.ch = ch
	w.statusch = statusch

	retrycount := 0
	sleepduration := []time.Duration{
		0,
		2 * time.Second,
		5 * time.Second,
		10 * time.Second,
		15 * time.Second,
	}

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

		if err := w.connect(); err != nil {
			w.reportStatus(false, fmt.Sprintf(
				"Connect to websocket error: %v",
				err,
			))
			continue
		}

		if err := w.subscribe(symbols); err != nil {
			w.reportStatus(false, fmt.Sprintf(
				"Subscribe websocket error: %v",
				err,
			))
			w.close()
			continue
		}

		w.reportStatus(true, "")

		if err := w.readLoop(); err != nil {
			w.reportStatus(false, fmt.Sprintf(
				"Read message error: %v",
				err,
			))
		}

		w.close()
	}
}

func (w *WsClient) reportStatus(healthy bool, errorMsg string) {
	w.statusch <- common.WsStatus{
		Exchange:  "lbankfutures",
		Timestamp: time.Now().UnixMicro(),
		IsHealthy: healthy,
		ErrorMsg:  errorMsg,
	}
}

func (w *WsClient) connect() error {
	header := http.Header{}
	header.Set("Origin", "https://www.lbank.com")
	header.Set(
		"User-Agent",
		"Mozilla/5.0 Chrome/150.0.0.0 Safari/537.36",
	)

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, response, err := dialer.Dial(w.baseurl, header)
	if err != nil {
		if response != nil {
			return fmt.Errorf(
				"websocket dial: %w, HTTP status: %s",
				err,
				response.Status,
			)
		}
		return err
	}

	w.conn = conn

	// 防止连接异常时一直阻塞写操作。
	w.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	return nil
}

func (w *WsClient) subscribe(symbols []string) error {
	w.symbolByTSN = make(map[string]string)

	extBytes, err := json.Marshal(map[string]any{
		"bvc":   "202",
		"isUsd": 1,
	})
	if err != nil {
		return fmt.Errorf("marshal ext: %w", err)
	}

	for index, inputSymbol := range symbols {
		symbol := strings.ToUpper(strings.TrimSpace(inputSymbol))
		if symbol == "" {
			continue
		}

		decimal := w.decimalBySymbol[symbol]
		if decimal == "" {
			decimal = defaultDepthDecimal
		}

		// 频道 3 的流水号以 3000000001 开始。
		tsn := strconv.FormatInt(
			3_000_000_001+int64(index),
			10,
		)

		instrumentID := fmt.Sprintf(
			"%s_%s_%d",
			symbol,
			decimal,
			depthLimit,
		)

		subscription := subscribeMsg{
			Ext:   string(extBytes),
			Topic: 3,
			TSN:   tsn,
			Parameter: subscribeParam{
				InstrumentID: instrumentID,
			},
			Type: 1,
		}

		if err := w.writeJSON(subscription); err != nil {
			return fmt.Errorf(
				"subscribe %s: %w",
				instrumentID,
				err,
			)
		}

		w.symbolByTSN[tsn] = symbol
	}

	if len(w.symbolByTSN) == 0 {
		return fmt.Errorf("no valid symbols")
	}

	return nil
}

func (w *WsClient) readLoop() error {
	heartbeatDone := make(chan struct{})
	defer close(heartbeatDone)

	go w.heartbeatLoop(heartbeatDone)

	for {
		if err := w.conn.SetReadDeadline(
			time.Now().Add(w.timeout),
		); err != nil {
			return err
		}

		_, msg, err := w.conn.ReadMessage()
		if err != nil {
			return err
		}

		receiveTime := time.Now().UnixMicro()

		var rawresp rawResponse
		if err := json.Unmarshal(msg, &rawresp); err != nil {
			// 心跳等非 Depth 消息不需要当作致命错误。
			slog.Debug(
				"ignore non-depth websocket message",
				"error", err,
				"exchange", w.baseurl,
			)
			continue
		}

		// 只处理 x=3 Depth。
		if rawresp.Topic != 3 {
			continue
		}

		// z=3 是初始快照，z=4 是持续快照。
		if rawresp.Type != 3 && rawresp.Type != 4 {
			if rawresp.Type == 14 {
				slog.Error(
					"lbank depth subscription rejected",
					"tsn", rawresp.TSN,
					"message", string(msg),
				)
			}
			continue
		}

		symbol := w.symbolByTSN[rawresp.TSN]
		if symbol == "" {
			slog.Warn(
				"unknown lbank depth subscription",
				"tsn", rawresp.TSN,
			)
			continue
		}

		// 用户只需要第一档；空订单簿必须先检查长度。
		if len(rawresp.Bids) == 0 || len(rawresp.Asks) == 0 {
			continue
		}

		ticker, err := rawresp.toTicker(symbol, scale)
		if err != nil {
			slog.Error(
				"websocket read loop error",
				"error", err,
				"exchange", w.baseurl,
				"symbol", symbol,
			)
			continue
		}

		ticker.ReceiveTime = receiveTime
		ticker.NormalizeTs()

		w.ch <- ticker
	}
}

func (w *WsClient) heartbeatLoop(done <-chan struct{}) {
	ticker := time.NewTicker(6 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return

		case now := <-ticker.C:
			heartbeat := heartbeatMsg{
				Action: "ping",
				Ping:   strconv.FormatInt(now.UnixMilli(), 10),
			}

			if err := w.writeJSON(heartbeat); err != nil {
				slog.Error(
					"lbank websocket heartbeat error",
					"error", err,
					"exchange", w.baseurl,
				)

				// 关闭连接，让 ReadMessage 返回并进入重连流程。
				w.close()
				return
			}
		}
	}
}

func (w *WsClient) writeJSON(value any) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	if w.conn == nil {
		return fmt.Errorf("websocket is not connected")
	}

	if err := w.conn.SetWriteDeadline(
		time.Now().Add(10 * time.Second),
	); err != nil {
		return err
	}

	return w.conn.WriteJSON(value)
}

func (w *WsClient) close() {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	if w.conn == nil {
		return
	}

	_ = w.conn.Close()
	w.conn = nil
}

func (r *rawResponse) toTicker(
	symbol string,
	scale int,
) (common.Ticker, error) {
	if len(r.Bids) == 0 {
		return common.Ticker{}, fmt.Errorf("empty bids")
	}

	if len(r.Asks) == 0 {
		return common.Ticker{}, fmt.Errorf("empty asks")
	}

	// LBank bid 从高到低排列，所以 Bids[0] 是最高买价。
	bidPriceStr := trimDecimalScale(r.Bids[0][0], scale)
	bidAmountStr := trimDecimalScale(r.Bids[0][1], scale)
	askPriceStr := trimDecimalScale(r.Asks[0][0], scale)
	askAmountStr := trimDecimalScale(r.Asks[0][1], scale)
	bidprice, err := common.Parsestringtoint64(
		bidPriceStr,
		scale,
	)
	if err != nil {
		return common.Ticker{}, err
	}

	bidamount, err := common.Parsestringtoint64(
		bidAmountStr,
		scale,
	)
	if err != nil {
		return common.Ticker{}, err
	}

	// LBank ask 从低到高排列，所以 Asks[0] 是最低卖价。
	askprice, err := common.Parsestringtoint64(
		askPriceStr,
		scale,
	)
	if err != nil {
		return common.Ticker{}, err
	}

	askamount, err := common.Parsestringtoint64(
		askAmountStr,
		scale,
	)
	if err != nil {
		return common.Ticker{}, err
	}

	ticker := common.Ticker{
		Exchange: "lbankfutures",
		Symbol:   symbol,

		PushTime: r.PushTime,

		BidPrice:  bidprice,
		BidAmount: bidamount,
		AskPrice:  askprice,
		AskAmount: askamount,
	}

	return ticker, nil
}

func trimDecimalScale(s string, scale int) string {
	s = strings.TrimSpace(s)

	dot := strings.IndexByte(s, '.')
	if dot == -1 {
		return s
	}

	if len(s)-dot-1 <= scale {
		return s
	}

	return s[:dot+1+scale]
}
