package main

import (
	"log"
	"time"

	"github.com/Rin18/bookticker-manager/aster/asterfutures"
	"github.com/Rin18/bookticker-manager/aster/asterspot"
)

func main() {
	manager := asterspot.NewManager()
	manager.Subscribe()
	managerf := asterfutures.NewManager()
	managerf.Subscribe()
	time.Sleep(10 * time.Second)
	for {
		time.Sleep(1 * time.Second)
		log.Printf("spot: %v\n", manager.GetTicker("BTCUSDT"))
		log.Printf("futures: %v\n", managerf.GetTicker("BTCUSDT"))
	}
	select {}
}
