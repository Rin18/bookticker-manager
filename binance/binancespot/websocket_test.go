package binancespot

import (
	"encoding/json"
	"log"
	"testing"

	"github.com/Rin18/bookticker-manager/common"
)

func TestListeningTicker(t *testing.T) {
	ch := make(chan respData, 50)
	statusch := make(chan common.WsStatus, 50)
	go listeningbookticker([]string{"btcusdt", "ethusdt", "dogeusdt"}, ch, statusch)
	for status := range statusch {
		log.Println(status)
	}
}

func BenchmarkToTicker(b *testing.B) {
	r := &respData{
		Symbol:    "BTCUSDT",
		BidPrice:  "50000.12345678",
		BidAmount: "1.500678123",
		AskPrice:  "50001.00012345",
		AskAmount: "2.10012345",
	}
	scale := 8

	// 2. 重置计时器，排除初始化数据的干扰
	b.ResetTimer()

	// 3. 执行循环测试
	for i := 0; i < b.N; i++ {
		_, _ = r.to_ticker(scale)
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	msg := []byte(`{"u":445981316,"s":"BTCUSDT","b":"50000.12345678","B":"1.500678123","a":"50001.00012345","A":"2.10012345"}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := respData{}
		_ = json.Unmarshal(msg, &r)
		r.to_ticker(8)
	}
}
