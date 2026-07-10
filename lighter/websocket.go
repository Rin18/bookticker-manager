package lighter

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/Rin18/bookticker-manager/common"
	"github.com/gorilla/websocket"
)

type subscribeMessage struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
}

type respQuote struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

type respTicker struct {
	Symbol        string    `json:"s"`
	Ask           respQuote `json:"a"`
	Bid           respQuote `json:"b"`
	LastUpdatedAt int64     `json:"last_updated_at"`
}

type respData struct {
	Channel         string     `json:"channel"`
	TransactionTime int64      `json:"last_updated_at"`
	Nonce           int64      `json:"nonce"`
	Ticker          respTicker `json:"ticker"`
	PushTime        int64      `json:"timestamp"`
	Type            string     `json:"type"`
}

func listeningbookticker(symbols []string, ch chan<- respData, statusch chan<- common.WsStatus) error {
	u := "wss://mainnet.zklighter.elliot.ai/stream"
	channels := buildchannels(symbols)
	retrycount := 0
	sleepduration := []time.Duration{0 * time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 15 * time.Second}
	lastconnect := time.Now()
RetryLoop:
	for {
		hasSentHealthy := false
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
		c, _, err := websocket.DefaultDialer.Dial(u, nil)
		if err != nil {
			statusch <- common.WsStatus{Exchange: "lighter", IsHealthy: false, ErrorMsg: "websocket dial failed: " + err.Error()}
			continue
		}

		for _, channel := range channels {
			msg := subscribeMessage{
				Type:    "subscribe",
				Channel: channel,
			}
			err = c.WriteJSON(msg)
			if err != nil {
				statusch <- common.WsStatus{Exchange: "lighter", IsHealthy: false, ErrorMsg: "websocket subscribe failed: " + err.Error()}
				continue RetryLoop
			}
		}

		const pongwait = 100 * time.Second
		c.SetReadDeadline(time.Now().Add(pongwait))
		c.SetPongHandler(func(appData string) error {
			c.SetReadDeadline(time.Now().Add(pongwait))
			return nil
		})

		// send ping
		go func() {
			for {
				time.Sleep(90 * time.Second)
				err := c.WriteMessage(websocket.PingMessage, nil)
				if err != nil {
					return
				}
			}
		}()

		// read messages
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				statusch <- common.WsStatus{Exchange: "lighter", IsHealthy: false, ErrorMsg: "websocket read failed: " + err.Error()}
				break
			}
			respdata := respData{}
			err = json.Unmarshal(msg, &respdata)
			if err != nil {
				statusch <- common.WsStatus{Exchange: "lighter", IsHealthy: false, ErrorMsg: "websocket unmarshal failed: " + err.Error()}
				break
			}
			if respdata.Type != "update/ticker" {
				continue
			}
			if !hasSentHealthy {
				statusch <- common.WsStatus{Exchange: "lighter", IsHealthy: true}
				hasSentHealthy = true
			}
			ch <- respdata
		}
	}
}

func (r *respData) to_ticker(scale int) (common.Ticker, error) {
	bidprice, err := common.Parsestringtoint64(r.Ticker.Bid.Price, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	bidamount, err := common.Parsestringtoint64(r.Ticker.Bid.Size, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askprice, err := common.Parsestringtoint64(r.Ticker.Ask.Price, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	askamount, err := common.Parsestringtoint64(r.Ticker.Ask.Size, scale)
	if err != nil {
		return common.Ticker{}, err
	}
	ticker := common.Ticker{Symbol: r.Ticker.Symbol, PushTime: r.PushTime, TransactionTime: r.TransactionTime, BidPrice: bidprice, BidAmount: bidamount, AskPrice: askprice, AskAmount: askamount}
	return ticker, nil
}

// helper functions
func buildchannels(symbols []string) []string {
	channels := []string{}
	for _, symbol := range symbols {
		symbol = strings.ToUpper(symbol)
		idx, found := convertsymboltoindex(symbol)
		if !found {
			continue
		}
		channel := "ticker/" + strconv.Itoa(idx)
		channels = append(channels, channel)
	}
	return channels
}

func convertsymboltoindex(symbol string) (int, bool) {
	var symbolToIndex = map[string]int{
		"BB":         211,
		"MINIMAX":    199,
		"AZTEC/USDC": 2055,
		"QCOM":       209,
		"CAP":        212,
		"SKY/USDC":   2053,
		"NOK":        208,
		"AVGO":       210,
		"LDO/USDC":   2054,
		"BYD":        200,
		"LIT/USDC":   2049,
		"LINK/USDC":  2050,
		"UNI/USDC":   2051,
		"ARM":        206,
		"ETH/USDC":   2048,
		"AAVE/USDC":  2052,
		"WEN":        214,
		"POPMART":    204,
		"ZHIPU":      205,
		"AAOI":       207,
		"BOT":        185,
		"RKLB":       186,
		"DELL":       187,
		"IBM":        188,
		"NBIS":       189,
		"QNT":        190,
		"NOW":        191,
		"OPENAI":     192,
		"ADI":        213,
		"TENCENT":    201,
		"SMIC":       202,
		"XIAOMI":     203,
		"ETH":        0,
		"BTC":        1,
		"SOL":        2,
		"DOGE":       3,
		"1000PEPE":   4,
		"WIF":        5,
		"WLD":        6,
		"XRP":        7,
		"LINK":       8,
		"AVAX":       9,
		"NEAR":       10,
		"DOT":        11,
		"GRAM":       12,
		"TAO":        13,
		"POL":        14,
		"TRUMP":      15,
		"SUI":        16,
		"1000SHIB":   17,
		"1000BONK":   18,
		"1000FLOKI":  19,
		"BERA":       20,
		"FARTCOIN":   21,
		"AI16Z":      22,
		"POPCAT":     23,
		"HYPE":       24,
		"BNB":        25,
		"JUP":        26,
		"AAVE":       27,
		"MKR":        28,
		"ENA":        29,
		"UNI":        30,
		"APT":        31,
		"SEI":        32,
		"KAITO":      33,
		"DATA":       34,
		"LTC":        35,
		"CRV":        36,
		"PENDLE":     37,
		"ONDO":       38,
		"ADA":        39,
		"S":          40,
		"VIRTUAL":    41,
		"SPX":        42,
		"TRX":        43,
		"SYRUP":      44,
		"PUMP":       45,
		"LDO":        46,
		"PENGU":      47,
		"PAXG":       48,
		"EIGEN":      49,
		"ARB":        50,
		"RESOLV":     51,
		"GRASS":      52,
		"ZORA":       53,
		"LAUNCHCOIN": 54,
		"OP":         55,
		"ZK":         56,
		"PROVE":      57,
		"BCH":        58,
		"HBAR":       59,
		"ZRO":        60,
		"GMX":        61,
		"DYDX":       62,
		"MNT":        63,
		"ETHFI":      64,
		"AERO":       65,
		"USELESS":    66,
		"TIA":        67,
		"MORPHO":     68,
		"VVV":        69,
		"YZY":        70,
		"XPL":        71,
		"WLFI":       72,
		"CRO":        73,
		"NMR":        74,
		"DOLO":       75,
		"LINEA":      76,
		"XMR":        77,
		"PYTH":       78,
		"SKY":        79,
		"MYX":        80,
		"1000TOSHI":  81,
		"AVNT":       82,
		"ASTER":      83,
		"0G":         84,
		"STBL":       85,
		"APEX":       86,
		"FF":         87,
		"2Z":         88,
		"EDEN":       89,
		"ZEC":        90,
		"MON":        91,
		"XAU":        92,
		"XAG":        93,
		"MEGA":       94,
		"MET":        95,
		"EURUSD":     96,
		"GBPUSD":     97,
		"USDJPY":     98,
		"USDCHF":     99,
		"USDCAD":     100,
		"CC":         101,
		"ICP":        102,
		"FIL":        103,
		"STRK":       104,
		"USDKRW":     105,
		"AUDUSD":     106,
		"NZDUSD":     107,
		"HOOD":       108,
		"COIN":       109,
		"NVDA":       110,
		"PLTR":       111,
		"TSLA":       112,
		"AAPL":       113,
		"AMZN":       114,
		"MSFT":       115,
		"GOOGL":      116,
		"META":       117,
		"STABLE":     118,
		"XLM":        119,
		"LIT":        120,
		"CRCL":       121,
		"MSTR":       122,
		"BMNR":       123,
		"FOGO":       124,
		"DUSK":       125,
		"RIVER":      126,
		"DASH":       127,
		"SPY":        128,
		"QQQ":        129,
		"SKR":        130,
		"AXS":        131,
		"ARC":        132,
		"BIRB":       133,
		"JTO":        134,
		"PIPPIN":     135,
		"XCU":        136,
		"INTC":       137,
		"AMD":        138,
		"SNDK":       139,
		"SAMSUNG":    140,
		"HYUNDAI":    141,
		"KRCOMP":     142,
		"SKHYNIX":    143,
		"AZTEC":      144,
		"WTI":        145,
		"XPD":        146,
		"XPT":        147,
		"HANMI":      148,
		"ROBO":       149,
		"URA":        150,
		"ASML":       151,
		"DIA":        152,
		"IWM":        153,
		"BOTZ":       154,
		"MAGS":       155,
		"STRC":       156,
		"EDGE":       157,
		"NATGAS":     158,
		"BRENTOIL":   159,
		"HYUNDAIUSD": 160,
		"SKHYNIXUSD": 161,
		"SAMSUNGUSD": 162,
		"CHIP":       163,
		"MU":         164,
		"ORCL":       165,
		"EWY":        166,
		"CRWV":       167,
		"TSM":        168,
		"SOXX":       169,
		"WHEAT":      170,
		"BIO":        171,
		"1000NOT":    172,
		"SPACEX":     173,
		"MRVL":       174,
		"CBRS":       175,
		"GME":        176,
		"BABA":       177,
		"LITE":       178,
		"TTWO":       179,
		"US500":      180,
		"US100":      181,
		"H100":       182,
		"CTR":        183,
		"RAIL":       184,
		"ANTHROPIC":  193,
		"SPCX":       194,
		"DRAM":       195,
		"BE":         196,
		"SOXL":       197,
		"USDHKD":     198,
	}
	index, exists := symbolToIndex[symbol]
	return index, exists
}
