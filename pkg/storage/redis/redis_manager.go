package redis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
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
	NodeTag    string
}

func NewRedisManager(ctx context.Context, client RedisClient, cfg *config.Config, luaScriptContent string) (*RedisManager, error) {
	scriptSHA, err := client.ScriptLoad(ctx, luaScriptContent).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to load Lua script to Redis: %v", err)
	}

	ctx, cancelFunc := context.WithCancel(ctx)
	logrus.Infof("RedisManager has been successfully initialized")
	return &RedisManager{
		client:     client,
		ctx:        ctx,
		cancelCtx:  cancelFunc,
		LuaSHA:     scriptSHA,
		TTLSeconds: cfg.Storage.Redis.RetentionTTLSeconds,
		NodeTag:    "node_tag",
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

// createRedisClient initializes a Redis client with optional ACL and SSL configurations
func CreateRedisClient(cfg *config.RedisConfig) (*redis.Client, error) {
	clientRequestTimeout, err := time.ParseDuration(cfg.ClientRequestTimeout)
	if err != nil {
		return nil, fmt.Errorf("invalid ClientRequestTimeout in Redis value: %v", err)
	}

	// Create the Redis client options
	redisOptions := &redis.Options{
		Addr:            fmt.Sprintf("%s:%d", cfg.Address, cfg.Port),
		DB:              0, // Use default DB
		ReadTimeout:     clientRequestTimeout,
		MaxRetries:      5,                      // Automatically retry up to 5 times
		MinRetryBackoff: 500 * time.Millisecond, // Initial backoff
		MaxRetryBackoff: 5 * time.Second,        // Maximum backoff
	}

	// If ACL is enabled, add username and password
	if cfg.Auth.Enabled {
		redisOptions.Username = cfg.Auth.Username
		redisOptions.Password = cfg.Auth.Password
	}

	// If SSL is enabled, configure TLS
	if cfg.SSL.Enabled {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: cfg.SSL.InsecureSkipVerify,
		}

		// Load client certificates if provided
		if cfg.SSL.ClientCertificateFile != "" && cfg.SSL.ClientKeyFile != "" {
			cert, err := tls.LoadX509KeyPair(cfg.SSL.ClientCertificateFile, cfg.SSL.ClientKeyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to load client certificate: %v", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		// Configure CA certificate if provided
		if cfg.SSL.CACertFile != "" {
			caCert, err := ioutil.ReadFile(cfg.SSL.CACertFile)
			if err != nil {
				return nil, fmt.Errorf("failed to load CA certificate: %v", err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.RootCAs = caCertPool
		}

		redisOptions.TLSConfig = tlsConfig
	}

	// Return the configured Redis client
	return redis.NewClient(redisOptions), nil
}
