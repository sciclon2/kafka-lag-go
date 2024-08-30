package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/sciclon2/kafka-lag-go/pkg/config"

	"github.com/redis/go-redis/v9"
)

// RedisClient defines the methods that the Redis manager should implement.
// This interface will allow us to mock the Redis client for testing.
type RedisClient interface {
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	Pipeline() redis.Pipeliner
	Close() error
	Ping(ctx context.Context) *redis.StatusCmd // Add Ping method to the interface
}

// RedisManager manages Redis operations, providing an interface for interacting with Redis.
type RedisManager struct {
	client     RedisClient
	ctx        context.Context
	cancelCtx  context.CancelFunc
	LuaSHA     string
	TTLSeconds int
}

func NewRedisManager(ctx context.Context, client RedisClient, cfg *config.Config, luaScriptContent string) (*RedisManager, error) {
	scriptSHA, err := client.ScriptLoad(ctx, luaScriptContent).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to load Lua script to Redis: %v", err)
	}

	ctx, cancelFunc := context.WithCancel(ctx)

	return &RedisManager{
		client:     client,
		ctx:        ctx,
		cancelCtx:  cancelFunc,
		LuaSHA:     scriptSHA,
		TTLSeconds: cfg.Storage.Redis.RetentionTTLSeconds,
	}, nil
}

// GracefulStop initiates a graceful shutdown of the RedisManager, ensuring that
// all ongoing operations are completed before the system stops.
func (rm *RedisManager) GracefulStop() error {
	// Cancel the context to signal any long-running operations to stop
	rm.cancelCtx()

	// Close the Redis client, which waits for pending commands to finish
	if err := rm.client.Close(); err != nil {
		return fmt.Errorf("failed to close Redis client: %w", err)
	}

	// Log the successful shutdown
	logrus.Infof("RedisManager has been gracefully stopped")
	return nil
}
