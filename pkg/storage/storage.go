package storage

import (
	"context"
	"fmt"
	"time"

	kafkaRedis "github.com/sciclon2/kafka-lag-go/pkg/storage/redis"

	"github.com/sciclon2/kafka-lag-go/pkg/config"

	// Alias custom Redis package
	"github.com/sciclon2/kafka-lag-go/pkg/structs"
)

// Storage interface defines the contract for various storage backends.
type Storage interface {
	RegisterNode(nodeID string, ttl int) (int, error)
	DeregisterNode(nodeID string) error
	GetNodeInfo(nodeID string) (int, int, error)
	StartNodeHeartbeat(nodeID string, heartbeatInterval time.Duration, ttl int)
	StartNodeMonitoring(monitorInterval time.Duration)
	PersistLatestProducedOffsets(groupsWithLeaderInfoAndLeaderOffsetsChan <-chan *structs.Group, numWorkers int) <-chan *structs.Group
	GracefulStop() error
	Ping(ctx ...context.Context) error
}

func InitializeStorage(cfg *config.Config) (Storage, error) {
	switch cfg.Storage.Type {
	case "redis":
		// Initialize the Redis client using the new function
		client, err := kafkaRedis.CreateRedisClient(&cfg.Storage.Redis)
		if err != nil {
			return nil, fmt.Errorf("failed to create Redis client: %v", err)
		}

		// Use a background context for the Redis client to avoid premature context cancellation
		redisCtx := context.Background()

		// Create and return a RedisManager instance
		manager, err := kafkaRedis.NewRedisManager(redisCtx, client, cfg, kafkaRedis.LuaScriptContent)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize Redis manager: %v", err)
		}

		return manager, nil
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", cfg.Storage.Type)
	}
}
