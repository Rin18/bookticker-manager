package common

type Ticker struct {
	Exchange  string
	Symbol    string
	UpdateId  int64
	BidPrice  int64
	BidAmount int64
	AskPrice  int64
	AskAmount int64
}
