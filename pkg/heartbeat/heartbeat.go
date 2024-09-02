package heartbeat

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/sciclon2/kafka-lag-go/pkg/kafka"
	"github.com/sciclon2/kafka-lag-go/pkg/storage"
	"github.com/sirupsen/logrus"
)

// ApplicationHeartbeat manages the health check process and status.
type ApplicationHeartbeat struct {
	KafkaAdmin      kafka.KafkaAdmin
	Store           storage.Storage
	Status          bool
	StatusLock      sync.RWMutex
	Interval        time.Duration
	HealthCheckPort int
	HealthCheckPath string
}

// NewApplicationHeartbeat initializes a new ApplicationHeartbeat instance.
func NewApplicationHeartbeat(kafkaAdmin kafka.KafkaAdmin, store storage.Storage, interval time.Duration, healthCheckPort int, healthCheckPath string) *ApplicationHeartbeat {
	return &ApplicationHeartbeat{
		KafkaAdmin:      kafkaAdmin,
		Store:           store,
		Status:          false, // Initial status is unhealthy until the first successful check
		Interval:        interval,
		HealthCheckPort: healthCheckPort,
		HealthCheckPath: healthCheckPath,
	}
}
func (ah *ApplicationHeartbeat) Start() {
	// Create a new ServeMux for health checks
	healthMux := http.NewServeMux()
	healthMux.HandleFunc(ah.HealthCheckPath, ah.HealthCheckHandler)

	// Start the HTTP server for health checks on the configured port in a separate goroutine
	go func() {
		address := fmt.Sprintf(":%d", ah.HealthCheckPort)
		logrus.Infof("Starting health check server on port %d with path %s", ah.HealthCheckPort, ah.HealthCheckPath)
		if err := http.ListenAndServe(address, healthMux); err != nil {
			logrus.Fatalf("Failed to start health check server: %v", err)
		}
	}()

	// Heartbeat check logic (unchanged)
	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			healthy := ah.checkKafka() && ah.checkRedis(ctx)

			ah.StatusLock.Lock()
			ah.Status = healthy
			ah.StatusLock.Unlock()

			if healthy {
				logrus.Debugf("Application heartbeat check passed")
			} else {
				logrus.Warn("Application heartbeat check failed")
			}

			time.Sleep(ah.Interval)
		}
	}()
}

// checkKafka performs a simple health check on the Kafka admin client.
func (ah *ApplicationHeartbeat) checkKafka() bool {
	// Attempt to list Kafka topics as a health check
	_, err := ah.KafkaAdmin.ListTopics()
	if err != nil {
		logrus.Errorf("Kafka health check failed: %v", err)
		return false
	}
	return true
}

// checkRedis performs a simple health check on the Redis store.
func (ah *ApplicationHeartbeat) checkRedis(ctx context.Context) bool {
	// Use the provided context for the Ping method
	err := ah.Store.Ping(ctx)
	if err != nil {
		logrus.Errorf("Redis health check failed: %v", err)
		return false
	}
	return true
}

func (ah *ApplicationHeartbeat) HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	ah.StatusLock.RLock()
	defer ah.StatusLock.RUnlock()

	if ah.Status {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("Unhealthy"))
	}
}
