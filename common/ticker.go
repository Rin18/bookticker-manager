package common

type Ticker struct {
	Exchange        string
	Symbol          string
	ReceiveTime     int64
	PushTime        int64
	TransactionTime int64
	BidPrice        int64
	BidAmount       int64
	AskPrice        int64
	AskAmount       int64
}

type WsStatus struct {
	Exchange  string
	Timestamp int64
	IsHealthy bool
	ErrorMsg  string
}
