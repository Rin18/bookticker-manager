package common

type Ticker struct {
	Exchange        string
	Symbol          string
	PushTime        int64
	TransactionTime int64
	BidPrice        int64
	BidAmount       int64
	AskPrice        int64
	AskAmount       int64
}
