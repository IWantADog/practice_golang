package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"parser/global"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

type processorFunc func(map[string]any) error

var processorFuncMap map[int32]processorFunc

func DefaultProcessor(data map[string]any) error {
	game_id := int32(data["game_id"].(float64))
	collectionName := genCollectionName(game_id)
	data["_id"] = data["draw_id"]
	test := int32(data["test"].(float64))
	if test == 1 {
		return errors.New("Test data")
	}
	err := InsertOneToDB(collectionName, data)
	return err
}

func genCollectionName(gameID int32) string {
	return fmt.Sprintf("game_%d", gameID)
}

func GetProcessor(gameID int32) processorFunc {
	if processor, ok := processorFuncMap[gameID]; ok {
		return processor
	}
	return DefaultProcessor
}

func ProcessorLoop(gID int32, wg *sync.WaitGroup, signal *bool, consumerPanic *chan int32) {
	global.Global.Logger.Info("Start processor loop", zap.Int32("game_id", gID))

	topic := fmt.Sprintf("game_%d", gID)
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "game_consumer_" + topic,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		global.Global.Logger.Error("Failed to create consumer", zap.Error(err), zap.String("topic", topic))
		panic(err)
	}

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		global.Global.Logger.Error("Failed to subscribe topic", zap.Error(err), zap.String("topic", topic))
		panic(err)
	}

	defer func() {
		if r := recover(); r != nil {
			(*consumerPanic) <- gID
		}
		c.Close()
		wg.Done()
	}()

	processor := GetProcessor(gID)
	for *signal {
		msg, err := c.ReadMessage(time.Second * 5)

		if err == nil {
			var data map[string]any

			err := json.Unmarshal(msg.Value, &data)
			if err != nil {
				global.Global.Logger.Error("Failed to unmarshal message", zap.Error(err), zap.String("message", string(msg.Value)))
				continue
			}
			global.Global.Logger.Info("Read message", zap.Any("data", data))
			err = processor(data)
			if err != nil {

				global.Global.Logger.Error("Failed to process data", zap.Error(err), zap.Any("data", data))
				panic(err)
			}
		} else {
			global.Global.Logger.Error("Failed to read message", zap.Error(err))
		}
	}

	global.Global.Logger.Info("Received signal to stop ProcessorLoop", zap.Int32("game_id", gID))
}
