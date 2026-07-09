package main

import (
	"fmt"
	"time"

	"github.com/Rin18/bookticker-manager/aster/asterspot"
)

func main() {
	manager := asterspot.NewManager()
	manager.Subscribe()
	time.Sleep(10 * time.Second)
	fmt.Println(manager.GetSymbolList())
	fmt.Println(manager.GetTicker("BTCUSDT"))
	select {}
}
