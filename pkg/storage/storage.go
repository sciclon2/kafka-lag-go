package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
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
	PersistLatestProducedOffsets(groupStructCompleteChan <-chan *structs.Group, groupStructCompleteAndPersistedChan chan<- *structs.Group, numWorkers int)
	GracefulStop() error
	Ping(ctx ...context.Context) error
}

func InitializeStorage(cfg *config.Config) (Storage, error) {
	switch cfg.Storage.Type {
	case "redis":
		clientRequestTimeout, err := time.ParseDuration(cfg.Storage.Redis.ClientRequestTimeout)

		if err != nil {
			return nil, fmt.Errorf("invalid ClientRequestTimeout in Redis value: %v", err)
		}
		// Use a background context for the Redis client to avoid premature context cancellation
		redisCtx := context.Background()
		// Create the Redis client
		client := redis.NewClient(&redis.Options{
			Addr:            cfg.GetRedisAddress(),
			Password:        "", // No password set
			DB:              0,  // Use default DB
			ReadTimeout:     clientRequestTimeout,
			MaxRetries:      5,                      // Automatically retry up to 3 times
			MinRetryBackoff: 500 * time.Millisecond, // Initial backoff
			MaxRetryBackoff: 5 * time.Second,        // Maximum backoff
		})

		// Create and return a RedisManager instance
		manager, err := kafkaRedis.NewRedisManager(redisCtx, client, cfg, kafkaRedis.LuaScriptContent)

		if err != nil {
			return nil, fmt.Errorf("failed to initialize Redis manager: %v", err)
		}
		return manager, nil

	// Add more storage types as needed

	default:
		return nil, fmt.Errorf("unsupported storage type: %s", cfg.Storage.Type)
	}
}
