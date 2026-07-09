package asterfutures

import "testing"

func TestListeningbookticker(t *testing.T) {
	ch := make(chan respData, 500)
	err := listeningbookticker([]string{"btcusdt", "ethusdt"}, ch)
	if err != nil {
		t.Errorf("error occured: %v", err)
	}
}
