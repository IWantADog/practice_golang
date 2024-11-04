package main

import (
	"context"
	"parser/global"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.uber.org/zap"
)

type Game struct {
	ID     bson.ObjectID `bson:"_id"`
	GameID int32         `bson:"game_id"`
	Name   string        `bson:"name"`
}

func GetAllGameIDFromDB() ([]int32, error) {
	global.Global.Logger.Info("Get all game id from db")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collection := global.Global.MongoDB.Database("game").Collection("game")
	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		global.Global.Logger.Error("Failed to get all game id", zap.Error(err))
		return nil, err
	}
	defer cursor.Close(ctx)

	var gameIDs []int32
	for cursor.Next(context.TODO()) {
		var gameInfo Game
		if err := cursor.Decode(&gameInfo); err != nil {
			panic(err)
		}
		gameIDs = append(gameIDs, gameInfo.GameID)
	}

	return gameIDs, nil
}

func InsertOneToDB(collectionName string, data map[string]any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	collection := global.Global.MongoDB.Database("game").Collection(collectionName)
	_, err := collection.InsertOne(ctx, data)
	return err
}
