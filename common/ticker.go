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

func (t *Ticker) NormalizeTs() {
	t.ReceiveTime = normalizeToUs(t.ReceiveTime)
	t.PushTime = normalizeToUs(t.PushTime)
	t.TransactionTime = normalizeToUs(t.TransactionTime)
}

func normalizeToUs(ts int64) int64 {
	if ts < 100_000_000_000 {
		return ts * 1_000_000
	}
	if ts < 100_000_000_000_000 {
		return ts * 1_000
	}
	if ts < 100_000_000_000_000_000 {
		return ts
	}
	return ts / 1_000
}
