package healthcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sciclon2/kafka-lag-go/pkg/storage"
	"github.com/sciclon2/kafka-lag-go/pkg/structs"

	"github.com/sirupsen/logrus"
)

/*
ApplicationHealthchech monitors the health of Kafka clusters and a Redis store
and provides an HTTP endpoint to expose the current health status.

Health Check Logic:
1. Kafka Health:
   - The health of each Kafka cluster is checked using the `ListTopics()` method.
   - If a Kafka cluster responds successfully, it is considered healthy.
   - If a Kafka cluster fails to respond or throws an error, it is marked as unhealthy
     and added to the `UnhealthyClusters` list.
   - The overall Kafka health is considered healthy if at least one Kafka cluster is healthy.

2. Redis Health:
   - Redis health is verified via the `Ping()` method.
   - If Redis responds successfully, it is considered healthy.
   - If Redis fails to respond, it is marked as unhealthy.

3. Overall Health Status:
   - The system is considered "healthy" if:
     - At least one Kafka cluster is healthy, and
     - Redis is healthy.
   - The system is considered "unhealthy" if:
     - All Kafka clusters are down, or
     - Redis is down.
   - The result of this health check is stored in the `Status` field (true = healthy, false = unhealthy)
     and the `UnhealthyClusters` list contains the names of any Kafka clusters that are down.

4. HTTP Health Check Endpoint:
   - The health status is exposed via an HTTP server at a configurable port and path (`HealthCheckPort` and `HealthCheckPath`).
   - The handler always returns a 200 OK response with a JSON body containing:
     - `status`: "OK" if the system is healthy, "Unhealthy" if the system is unhealthy.
     - `unhealthy_clusters`: A list of Kafka clusters that are down. This list is always included, even if the system is healthy.

5. Health Check Frequency:
   - The health checks are performed at intervals defined by the `Interval` field.
   - The results of these checks are updated periodically, ensuring up-to-date health status reporting.
*/

type ApplicationHealthchech struct {
	KafkaAdmins       map[string]structs.KafkaAdmin
	Store             storage.Storage
	Status            bool
	UnhealthyClusters []string // List of unhealthy Kafka clusters
	StatusLock        sync.RWMutex
	Interval          time.Duration
	HealthCheckPort   int
	HealthCheckPath   string
}

func NewApplicationHealthcheck(kafkaAdmins map[string]structs.KafkaAdmin, store storage.Storage, interval time.Duration, healthCheckPort int, healthCheckPath string) *ApplicationHealthchech {
	return &ApplicationHealthchech{
		KafkaAdmins:     kafkaAdmins,
		Store:           store,
		Status:          false, // Initial status is unhealthy until the first successful check
		Interval:        interval,
		HealthCheckPort: healthCheckPort,
		HealthCheckPath: healthCheckPath,
	}
}

func (ah *ApplicationHealthchech) Start() {
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

	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			kafkaHealthy := ah.checkKafka()
			redisHealthy := ah.checkRedis(ctx)

			ah.StatusLock.Lock()
			// The system is only unhealthy if Redis is down or all Kafka clusters are down
			ah.Status = kafkaHealthy && redisHealthy // System is healthy if both Kafka and Redis are healthy
			ah.StatusLock.Unlock()
			if ah.Status {
				logrus.Debugf("Application heartbeat check passed")
			} else {
				logrus.Warn("Application heartbeat check failed")
			}

			time.Sleep(ah.Interval)
		}
	}()
}

func (ah *ApplicationHealthchech) checkKafka() bool {
	var healthyCount int32
	var unhealthyClusters []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	for clusterName, admin := range ah.KafkaAdmins {
		wg.Add(1)
		go func(clusterName string, admin structs.KafkaAdmin) {
			defer wg.Done()

			_, err := admin.ListTopics()
			if err != nil {
				logrus.Warnf("Kafka health check failed for cluster %s: %v", clusterName, err)
				mu.Lock()
				unhealthyClusters = append(unhealthyClusters, clusterName)
				mu.Unlock()
			} else {
				// Atomically increment the healthyCount
				atomic.AddInt32(&healthyCount, 1)
			}
		}(clusterName, admin)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Update the unhealthy clusters in a thread-safe manner
	ah.StatusLock.Lock()
	ah.UnhealthyClusters = unhealthyClusters
	ah.StatusLock.Unlock()

	// Return true if at least one Kafka cluster is healthy
	return healthyCount > 0
}

// checkRedis performs a simple health check on the Redis store.
func (ah *ApplicationHealthchech) checkRedis(ctx context.Context) bool {
	// Use the provided context for the Ping method
	err := ah.Store.Ping(ctx)
	if err != nil {
		logrus.Errorf("Redis health check failed: %v", err)
		return false
	}
	return true
}

func (ah *ApplicationHealthchech) HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	ah.StatusLock.RLock()
	defer ah.StatusLock.RUnlock()

	response := structs.HealthStatus{
		Status:            "OK",
		UnhealthyClusters: ah.UnhealthyClusters, // Always include the unhealthy clusters
	}

	// If the system is unhealthy (either Redis or all Kafka clusters are down), change the status
	if !ah.Status {
		response.Status = "Unhealthy"
	}

	// Log the response for debugging purposes
	logrus.Infof("HealthCheckHandler response: %+v", response)

	// Always return 200 OK status, with the health status in the body
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
