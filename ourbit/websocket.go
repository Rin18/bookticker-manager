package ourbit

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Rin18/bookticker-manager/common"
	"github.com/gorilla/websocket"
)

const (
	baseurl           = "wss://futures.ourbit.com/edge"
	origin            = "https://futures.ourbit.com"
	contractDetailURL = "https://futures.ourbit.com/api/v1/contract/detail"
	scale             = 8
	heartbeatDefault  = 15 * time.Second
)

type WsClient struct {
	baseurl  string
	conn     *websocket.Conn
	timeout  time.Duration
	ch       chan<- common.Ticker
	statusch chan<- common.WsStatus

	// contractSpecs maps an Ourbit symbol to its volume conversion metadata.
	// The values are loaded once before the websocket is connected and are
	// treated as immutable afterwards, so concurrent reads are safe.
	contractSpecs map[string]contractSpec
}

type request struct {
	Method string         `json:"method"`
	Param  map[string]any `json:"param,omitempty"`
}

// envelope is used first because subscription acknowledgements and pong
// messages do not have the same data shape as depth messages.
type envelope struct {
	Symbol   string          `json:"symbol"`
	Channel  string          `json:"channel"`
	Data     json.RawMessage `json:"data"`
	PushTime int64           `json:"ts"`
}

// level is [price, volume in contracts, number of orders].
type level [3]json.Number

type depthData struct {
	TransactionTime     int64       `json:"ct"`
	AskMarketLevelPrice json.Number `json:"askMarketLevelPrice"`
	// encoding/json discards JSON array elements beyond the Go array length,
	// so only the first (best) level is retained from the 40-level snapshot.
	Asks                [1]level    `json:"asks"`
	Bids                [1]level    `json:"bids"`
	UpdateID            int64       `json:"version"`
	BidMarketLevelPrice json.Number `json:"bidMarketLevelPrice"`
}

type rawResponse struct {
	Symbol   string    `json:"symbol"`
	Channel  string    `json:"channel"`
	PushTime int64     `json:"ts"`
	Data     depthData `json:"data"`
}

type snapshotKey struct {
	UpdateID        int64
	TransactionTime int64
	PushTime        int64
}

type contractDetailResponse struct {
	Data []contractDetail `json:"data"`
}

type contractDetail struct {
	Symbol       string      `json:"symbol"`
	ContractSize json.Number `json:"contractSize"`
	VolScale     int         `json:"volScale"`
}

type contractSpec struct {
	// ContractSize is base-asset units per contract, stored at scale=8.
	ContractSize int64
	// VolScale is the decimal precision of websocket contract volume.
	VolScale int
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

		if err := w.ensureContractSizes(symbols); err != nil {
			statusch <- common.WsStatus{
				Exchange:  "ourbitfutures",
				Timestamp: time.Now().UnixMicro(),
				IsHealthy: false,
				ErrorMsg:  fmt.Sprintf("Load contract details error: %v", err),
			}
			continue
		}

		if err := w.connect(); err != nil {
			statusch <- common.WsStatus{
				Exchange:  "ourbitfutures",
				Timestamp: time.Now().UnixMicro(),
				IsHealthy: false,
				ErrorMsg:  fmt.Sprintf("Connect to websocket error: %v", err),
			}
			continue
		}

		if err := w.subscribe(symbols); err != nil {
			_ = w.conn.Close()
			statusch <- common.WsStatus{
				Exchange:  "ourbitfutures",
				Timestamp: time.Now().UnixMicro(),
				IsHealthy: false,
				ErrorMsg:  fmt.Sprintf("Subscribe websocket error: %v", err),
			}
			continue
		}

		statusch <- common.WsStatus{
			Exchange:  "ourbitfutures",
			Timestamp: time.Now().UnixMicro(),
			IsHealthy: true,
			ErrorMsg:  "",
		}

		err := w.readLoop()
		_ = w.conn.Close()
		if err != nil {
			statusch <- common.WsStatus{
				Exchange:  "ourbitfutures",
				Timestamp: time.Now().UnixMicro(),
				IsHealthy: false,
				ErrorMsg:  fmt.Sprintf("Read message error: %v", err),
			}
		}
	}
}

func (w *WsClient) ensureContractSizes(symbols []string) error {
	if w.contractSpecs == nil {
		sizes, err := loadContractSizes()
		if err != nil {
			return err
		}
		w.contractSpecs = sizes
	}

	for _, symbol := range symbols {
		ourbitSymbol, err := normalizeOurbitSymbol(symbol)
		if err != nil {
			return err
		}
		if _, ok := w.contractSpecs[ourbitSymbol]; !ok {
			return fmt.Errorf("contractSize not found for %s", ourbitSymbol)
		}
	}
	return nil
}

func loadContractSizes() (map[string]contractSpec, error) {
	req, err := http.NewRequest(http.MethodGet, contractDetailURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Origin", origin)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("contract detail HTTP status: %s", resp.Status)
	}

	var result contractDetailResponse
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}

	sizes := make(map[string]contractSpec, len(result.Data))
	for _, contract := range result.Data {
		symbol := strings.ToUpper(strings.TrimSpace(contract.Symbol))
		if symbol == "" || contract.ContractSize == "" {
			continue
		}
		if contract.VolScale < 0 || contract.VolScale > scale {
			return nil, fmt.Errorf("unsupported volScale %d for %s", contract.VolScale, symbol)
		}
		size, err := common.Parsestringtoint64(contract.ContractSize.String(), scale)
		if err != nil || size <= 0 {
			return nil, fmt.Errorf(
				"invalid contractSize %q for %s: %v",
				contract.ContractSize.String(),
				symbol,
				err,
			)
		}
		sizes[symbol] = contractSpec{ContractSize: size, VolScale: contract.VolScale}
	}
	if len(sizes) == 0 {
		return nil, errors.New("contract detail response contains no contractSize values")
	}
	return sizes, nil
}

func (w *WsClient) connect() error {
	header := http.Header{}
	header.Set("Origin", origin)

	c, _, err := websocket.DefaultDialer.Dial(w.baseurl, header)
	if err != nil {
		return err
	}
	w.conn = c
	return nil
}

func (w *WsClient) subscribe(symbols []string) error {
	// sub.depth.step does not support one request containing multiple symbols,
	// so send one subscription message per symbol. Omitting step makes the
	// server select that contract's minimum/native price step.
	for _, symbol := range symbols {
		ourbitSymbol, err := normalizeOurbitSymbol(symbol)
		if err != nil {
			return err
		}
		msg := request{
			Method: "sub.depth.step",
			Param: map[string]any{
				"symbol": ourbitSymbol,
			},
		}
		if err := w.conn.WriteJSON(msg); err != nil {
			return err
		}
	}
	return nil
}

func (w *WsClient) readLoop() error {
	heartbeatStop := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go w.heartbeatLoop(heartbeatStop, heartbeatDone)
	defer func() {
		close(heartbeatStop)
		<-heartbeatDone
	}()

	lastBySymbol := make(map[string]snapshotKey, 16)

	for {
		if w.timeout > 0 {
			if err := w.conn.SetReadDeadline(time.Now().Add(w.timeout)); err != nil {
				return err
			}
		}

		_, msg, err := w.conn.ReadMessage()
		if err != nil {
			return err
		}
		receiveTime := time.Now().UnixMicro()

		var env envelope
		if err := json.Unmarshal(msg, &env); err != nil {
			slog.Error(
				"websocket read loop error",
				"json unmarshal error", err,
				"exchange", w.baseurl,
			)
			continue
		}

		switch env.Channel {
		case "rs.sub.depth.step", "pong":
			continue
		case "rs.error", "rs.error.depth.step":
			slog.Error(
				"ourbit websocket server error",
				"channel", env.Channel,
				"data", string(env.Data),
			)
			continue
		case "push.depth.step":
			// Continue below.
		default:
			continue
		}

		var rawresp rawResponse
		if err := json.Unmarshal(msg, &rawresp); err != nil {
			slog.Error(
				"websocket read loop error",
				"depth unmarshal error", err,
				"exchange", w.baseurl,
			)
			continue
		}

		if rawresp.Symbol == "" || rawresp.Data.Bids[0][0] == "" || rawresp.Data.Asks[0][0] == "" {
			continue
		}

		// The step=10 stream has been observed sending consecutive identical
		// snapshots. Dedupe every step defensively.
		key := snapshotKey{
			UpdateID:        rawresp.Data.UpdateID,
			TransactionTime: rawresp.Data.TransactionTime,
			PushTime:        rawresp.PushTime,
		}
		if last, ok := lastBySymbol[rawresp.Symbol]; ok && key == last {
			continue
		}
		lastBySymbol[rawresp.Symbol] = key

		contractSpec, ok := w.contractSpecs[rawresp.Symbol]
		if !ok {
			slog.Error(
				"websocket read loop error",
				"contractSize not found", rawresp.Symbol,
				"exchange", w.baseurl,
			)
			continue
		}

		ticker, err := rawresp.toTicker(scale, contractSpec)
		if err != nil {
			slog.Error(
				"websocket read loop error",
				"to ticker error", err,
				"exchange", w.baseurl,
			)
			continue
		}
		ticker.ReceiveTime = receiveTime
		ticker.NormalizeTs()
		w.ch <- ticker
	}
}

func (w *WsClient) heartbeatLoop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	interval := heartbeatDefault
	if w.timeout > 0 && w.timeout/2 < interval {
		interval = w.timeout / 2
	}
	if interval < time.Second {
		interval = time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if err := w.conn.WriteJSON(request{Method: "ping"}); err != nil {
				// Closing the connection wakes ReadMessage so Start can reconnect.
				_ = w.conn.Close()
				return
			}
		}
	}
}

func (r *rawResponse) toTicker(scale int, spec contractSpec) (common.Ticker, error) {
	if r.Data.Bids[0][0] == "" || r.Data.Asks[0][0] == "" {
		return common.Ticker{}, errors.New("empty order book")
	}

	// sub.depth.step sends up to 40 complete levels per side. BookTicker only
	// needs index 0, which is the best bid/ask.
	bestBid := r.Data.Bids[0]
	bestAsk := r.Data.Asks[0]

	bidprice, err := common.Parsestringtoint64(bestBid[0].String(), scale)
	if err != nil {
		return common.Ticker{}, err
	}
	// Depth volume is expressed in contracts. Convert it to base-coin units:
	// base amount = contracts * contractSize. The result remains an int64
	// fixed-point value using the same scale as prices.
	bidamount, err := contractsToBaseAmount(bestBid[1].String(), spec)
	if err != nil {
		return common.Ticker{}, err
	}
	askprice, err := common.Parsestringtoint64(bestAsk[0].String(), scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askamount, err := contractsToBaseAmount(bestAsk[1].String(), spec)
	if err != nil {
		return common.Ticker{}, err
	}

	return common.Ticker{
		Exchange:        "ourbitfutures",
		Symbol:          strings.ReplaceAll(r.Symbol, "_", ""),
		PushTime:        r.PushTime,
		TransactionTime: r.Data.TransactionTime,
		BidPrice:        bidprice,
		BidAmount:       bidamount,
		AskPrice:        askprice,
		AskAmount:       askamount,
	}, nil
}

func contractsToBaseAmount(contracts string, spec contractSpec) (int64, error) {
	if spec.ContractSize <= 0 {
		return 0, errors.New("invalid contractSize")
	}

	// Parse volume using the exchange-provided volScale instead of scale=8.
	// For all currently listed Ourbit contracts volScale is 0, so a value such
	// as "1308247" remains 1308247 rather than being prematurely multiplied
	// by 1e8.
	contractVolume, err := common.Parsestringtoint64(contracts, spec.VolScale)
	if err != nil {
		return 0, fmt.Errorf("parse contract volume %q: %w", contracts, err)
	}
	if contractVolume < 0 {
		return 0, fmt.Errorf("negative contract volume %q", contracts)
	}

	divisor, err := power10Int64(spec.VolScale)
	if err != nil {
		return 0, err
	}

	// Desired fixed-point result:
	//   contractVolume * contractSize(scale=8) / 10^volScale
	// Cancel the divisor before multiplying. This avoids intermediate overflow
	// without float64 or math/big and preserves exact fixed-point arithmetic.
	a := contractVolume
	b := spec.ContractSize
	g := greatestCommonDivisor(a, divisor)
	a /= g
	divisor /= g
	g = greatestCommonDivisor(b, divisor)
	b /= g
	divisor /= g
	if divisor != 1 {
		return 0, fmt.Errorf("base amount %q cannot be represented exactly at scale %d", contracts, scale)
	}

	const maxInt64 = int64(^uint64(0) >> 1)
	if a != 0 && b > maxInt64/a {
		return 0, fmt.Errorf("base amount overflows int64 for volume %q", contracts)
	}
	return a * b, nil
}

func greatestCommonDivisor(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	if a == 0 {
		return 1
	}
	return a
}

func power10Int64(decimalScale int) (int64, error) {
	if decimalScale < 0 || decimalScale > 18 {
		return 0, fmt.Errorf("unsupported decimal scale %d", decimalScale)
	}
	result := int64(1)
	for i := 0; i < decimalScale; i++ {
		result *= 10
	}
	return result, nil
}

func normalizeOurbitSymbol(symbol string) (string, error) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	symbol = strings.ReplaceAll(symbol, "-", "_")
	symbol = strings.ReplaceAll(symbol, "/", "_")

	if symbol == "" {
		return "", errors.New("empty symbol")
	}
	if strings.Contains(symbol, "_") {
		return symbol, nil
	}
	if strings.HasSuffix(symbol, "USDT") && len(symbol) > len("USDT") {
		return strings.TrimSuffix(symbol, "USDT") + "_USDT", nil
	}
	return "", fmt.Errorf("cannot convert symbol %q to Ourbit BASE_QUOTE format", symbol)
}
