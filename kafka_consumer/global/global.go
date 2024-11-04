package global

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.uber.org/zap"
)

type GlobalModule struct {
	MongoDB *mongo.Client
	Logger  *zap.Logger
}

var Global GlobalModule

func (m *GlobalModule) Init() {
	opts := options.Client().ApplyURI("mongodb://root:123456@localhost:27017")
	db, err := mongo.Connect(opts)
	if err != nil {
		panic(err)
	}
	m.MongoDB = db
	_ = m.MongoDB

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	m.Logger = logger
	_ = m.Logger
}

func (m *GlobalModule) Run() {}

func (m *GlobalModule) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := m.MongoDB.Disconnect(ctx); err != nil {
		panic(err)
	}
}
