package binancespot

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/gorilla/websocket"
)

type RespData struct {
	UpdateId  int64  `json:"u"`
	Symbol    string `json:"s"`
	BidPrice  string `json:"b"`
	BidAmount string `json:"B"`
	AskPrice  string `json:"a"`
	AskAmount string `json:"A"`
}

func Listeningbookticker(symbols []string, ch chan<- RespData) error {
	params := buildparamshelper(symbols)
	u := "wss://stream.binance.com:9443/ws/" + params
	c, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		return err
	}
	defer c.Close()
	for {
		_, msg, err := c.ReadMessage()
		if err != nil {
			return err
		}
		respdata := RespData{}
		err = json.Unmarshal(msg, &respdata)
		if err != nil {
			return err
		}
		ch <- respdata
	}
}

func buildparamshelper(symbols []string) string {
	params := []string{}
	for _, symbol := range symbols {
		lowercasedSymbol := strings.ToLower(symbol)
		streamName := lowercasedSymbol + "@bookTicker"
		params = append(params, streamName)
	}
	return strings.Join(params, "/")
}

func (r *RespData) to_ticker(scale int) (Ticker, error) {
	bidprice, err := parsestringtoint64(r.BidPrice, scale)
	if err != nil {
		return Ticker{}, err
	}
	bidamount, err := parsestringtoint64(r.BidAmount, scale)
	if err != nil {
		return Ticker{}, err
	}
	askprice, err := parsestringtoint64(r.AskPrice, scale)
	if err != nil {
		return Ticker{}, err
	}
	askamount, err := parsestringtoint64(r.AskAmount, scale)
	if err != nil {
		return Ticker{}, err
	}
	ticker := Ticker{symbol: r.Symbol, updateid: r.UpdateId, bidprice: bidprice, bidamount: bidamount, askprice: askprice, askamount: askamount}
	return ticker, nil
}

func parsestringtoint64(s string, scale int) (int64, error) {
	multiple := power10(scale)
	parts := strings.Split(s, ".")
	if len(parts) == 1 {
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("Error parse %v to int: %v", s, err)
		}
		return i * multiple, nil
	}
	if len(parts) != 2 {
		return 0, fmt.Errorf("Incorrect number of . in %v", s)
	}

	integerPart, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing integer part: %v", err)
	}
	decimalPart := parts[1]
	if len(decimalPart) > scale {
		return 0, fmt.Errorf("decimal length %v larger than %v", len(decimalPart), scale)
	} else if len(decimalPart) < scale {
		contatenationAmount := scale - len(decimalPart)
		decimalPart += strings.Repeat("0", contatenationAmount)
	}
	decimalPartInt, err := strconv.ParseInt(decimalPart, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing decimal part %v to int: %v", decimalPart, err)
	}
	return integerPart*multiple + decimalPartInt, nil

}

func power10(scale int) int64 {
	number := 1
	for i := 0; i < scale; i++ {
		number *= 10
	}
	return int64(number)
}
