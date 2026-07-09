package lighter

import (
	"log"
	"testing"
)

func TestListeningbookticker(t *testing.T) {
	ch := make(chan RespData, 500)
	err := Listeningbookticker([]string{"btc", "eth", "doge", "sol"}, ch)
	if err != nil {
		t.Errorf("error occured: %v", err)
	}
}

func TestBuildChannels(t *testing.T) {
	symbols := []string{"btc", "eth", "doge"}
	channels := buildchannels(symbols)
	want := []string{"ticker/1", "ticker/0", "ticker/3"}
	log.Println(channels)
	log.Println(want)
}
