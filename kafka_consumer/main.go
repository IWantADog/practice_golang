package main

import (
	"os"
	"os/signal"
	"parser/base"
	"parser/global"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
)

var processorStatusMap map[int32]bool

var updateConsumerChan chan int32

func UpdateConsumer(signal *chan os.Signal) {
	currentGameIDs, err := GetAllGameIDFromDB()
	if err != nil {
		panic(err)
	}

	updateConsumerChan = make(chan int32, len(currentGameIDs)*2)
	gracefulShutdown := true
	var wg sync.WaitGroup

	if processorStatusMap == nil {
		processorStatusMap = make(map[int32]bool)
		for _, gID := range currentGameIDs {
			processorStatusMap[gID] = true
			wg.Add(1)
			go ProcessorLoop(gID, &wg, &gracefulShutdown, &updateConsumerChan)
		}
	}
	time.Sleep(5 * time.Second)

	for {
		select {
		case <-*signal:
			gracefulShutdown = false
			global.Global.Logger.Info("prepare to stop all consumer")
			wg.Wait()
			global.Global.Logger.Info("Received signal to stop UpdateConsumer")
			return
		case gID := <-updateConsumerChan:
			processorStatusMap[gID] = false
			global.Global.Logger.Info("Processor stopped", zap.Int32("game_id", gID))
		default:
			currentGameIDs, err := GetAllGameIDFromDB()
			if err != nil {
				time.Sleep(10 * time.Second)
			}
			for _, gID := range currentGameIDs {
				status, ok := processorStatusMap[gID]
				if !ok || !status {
					processorStatusMap[gID] = true
					wg.Add(1)
					go ProcessorLoop(gID, &wg, &gracefulShutdown, &updateConsumerChan)
				}
			}
			time.Sleep(5 * time.Second)
		}
	}
}

func main() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	modules := []base.Module{
		&global.Global,
	}

	for _, m := range modules {
		m.Init()
	}
	UpdateConsumer(&signalChan)

	for _, m := range modules {
		m.Close()
	}
}
