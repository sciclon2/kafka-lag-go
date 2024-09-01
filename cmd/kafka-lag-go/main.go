package main

import (
	"log"
	"os"
	"time"

	"github.com/sciclon2/kafka-lag-go/pkg/config"
	maininit "github.com/sciclon2/kafka-lag-go/pkg/init"

	"github.com/sciclon2/kafka-lag-go/pkg/heartbeat"
	"github.com/sciclon2/kafka-lag-go/pkg/kafka"
	"github.com/sciclon2/kafka-lag-go/pkg/metrics"
	"github.com/sciclon2/kafka-lag-go/pkg/storage"
	"github.com/sciclon2/kafka-lag-go/pkg/structs"

	_ "net/http/pprof"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// Constants for health check intervals
const (
	heartbeatInterval = 5 * time.Second
	monitorInterval   = 10 * time.Second
)

// generateNodeID creates a unique identifier for the node using UUID.
func generateNodeID() string {
	return uuid.New().String()
}

func main() {
	// Initialize signal handling
	sigChan := maininit.InitializeSignalHandling()

	cfg := maininit.InitializeConfigAndLogging()

	// Set up logging
	cfg.SetLogLevel()

	// Initialize Kafka client and admin
	client, admin, saramaConfig := maininit.InitializeKafkaClient(cfg)
	defer client.Close()
	defer admin.Close()

	// Set up and start Prometheus metrics HTTP server.
	maininit.InitializeMetricsServer(cfg)

	// Initialize the Storage interface using the function from the storage package
	store := maininit.InitializeStorage(cfg)
	defer store.GracefulStop()

	// Initialize and start the ApplicationHeartbeat
	initializeAndStartHeartbeat(admin, store, heartbeatInterval*time.Second, cfg)

	// Generate a unique ID for this node.
	nodeID := generateNodeID()

	// Register the node in Redis with a TTL (time-to-live) to indicate it's active.
	registeredNodeIndex, err := store.RegisterNode(nodeID, 10)
	if err != nil {
		logrus.Fatalf("Failed to register node: %v", err)
	}
	defer func() {
		// Deregister the node upon application exit.
		if err := store.DeregisterNode(nodeID); err != nil {
			logrus.Fatalf("Failed to deregister node: %v", err)
		}
	}()

	logrus.Infof("Node registered with ID %s", nodeID)

	// Set the iteration interval for processing loops.
	iterationInterval, err := cfg.GetIterationInterval()
	if err != nil {
		logrus.Fatalf("Invalid iteration interval: %v", err)
	}

	// Start the node heartbeat
	store.StartNodeHeartbeat(nodeID, heartbeatInterval, 10)

	// Start monitoring other nodes
	store.StartNodeMonitoring(monitorInterval)

	// Initialize Prometheus metrics
	prometheusMetrics := metrics.NewPrometheusMetrics(cfg.Prometheus.Labels)
	prometheusMetrics.RegisterMetrics()

	// Main processing loop that runs continuously.
	for {
		startTime := time.Now()
		logrus.Infof("Starting new iteration")

		// Fetch the list of active nodes from Redis.
		currentNodeIndex, totalNodes, err := store.GetNodeInfo(nodeID)
		if err != nil {
			logrus.Fatalf("Failed to get node information: %v", err)
		}

		// Log if the node index has changed, indicating a change in the cluster (node added/removed)
		if currentNodeIndex != registeredNodeIndex {
			logrus.Infof("Node index updated: Previous index: %d, New index: %d, Total nodes: %d", registeredNodeIndex, currentNodeIndex, totalNodes)
			registeredNodeIndex = currentNodeIndex
		} else {
			logrus.Debugf("Total nodes: %d, Current node index: %d", totalNodes, registeredNodeIndex)
		}

		// Channels for handling consumer group data and results, and a WaitGroup for synchronization.
		groupNameChan := make(chan string)
		groupStructPartialChan := make(chan *structs.Group)
		groupStructCompleteChan := make(chan *structs.Group)
		groupStructCompleteAndPersistedChan := make(chan *structs.Group)
		metricsToExportChan := make(chan *structs.Group)

		// Start fetching consumer groups
		kafka.FetchConsumerGroups(admin, groupNameChan, cfg)

		// Fetch and describe group topics
		kafka.GetConsumerGroupsInfo(admin, client, groupNameChan, groupStructPartialChan, cfg.App.NumWorkers, registeredNodeIndex, totalNodes)

		// Process group offsets
		kafka.GetLatestProducedOffsets(admin, groupStructPartialChan, groupStructCompleteChan, cfg.App.NumWorkers, saramaConfig)

		store.PersistLatestProducedOffsets(groupStructCompleteChan, groupStructCompleteAndPersistedChan, cfg.App.NumWorkers)

		// Create an instance of LagProcessor
		lp := metrics.NewLagProcessor()
		lp.GenerateMetrics(groupStructCompleteAndPersistedChan, metricsToExportChan, cfg.App.NumWorkers)

		// Start processing metrics concurrently
		prometheusMetrics.ProcessMetrics(metricsToExportChan, cfg.App.NumWorkers, startTime)

		// Wait for the next iteration or handle a signal
		SleepToMaintainInterval(startTime, iterationInterval, sigChan)
	}
}

// SleepToMaintainInterval ensures the iteration runs at the configured interval,
// but can be interrupted by a user signal to proceed immediately.
func SleepToMaintainInterval(startTime time.Time, iterationInterval time.Duration, sigChan chan os.Signal) {
	// Calculate the next expected start time
	nextExpectedStartTime := startTime.Add(iterationInterval)
	currentTime := time.Now()

	// Calculate the remaining time to wait, if any
	sleepDuration := nextExpectedStartTime.Sub(currentTime)

	if sleepDuration > 0 {
		log.Printf("Iteration completed early. Waiting for %v until next iteration or a signal to proceed immediately.", sleepDuration)

		// Create a Timer instead of using time.After
		timer := time.NewTimer(sleepDuration)

		select {
		case <-timer.C:
			// The wait time has passed, proceed with the next iteration
			log.Println("Wait time elapsed. Proceeding with the next iteration.")
		case <-sigChan:
			// Signal received, force an immediate iteration
			if !timer.Stop() {
				<-timer.C // Drain the timer channel if necessary
			}
			log.Println("Received signal to proceed immediately. Skipping wait.")
		}
	} else {
		log.Printf("Iteration took longer than the interval. Starting next iteration immediately.")
	}

	// Log the final timing for this iteration
	totalElapsedTime := time.Since(startTime)
	log.Printf("Total time elapsed for this iteration including wait time: %v", totalElapsedTime)
}

// initializeAndStartHeartbeat initializes the ApplicationHeartbeat and starts the health check routine.
func initializeAndStartHeartbeat(kafkaAdmin kafka.KafkaAdmin, store storage.Storage, interval time.Duration, cfg *config.Config) *heartbeat.ApplicationHeartbeat {
	applicationHeartbeat := heartbeat.NewApplicationHeartbeat(kafkaAdmin, store, interval, cfg.App.HealthCheckPort, cfg.App.HealthCheckPath)
	applicationHeartbeat.Start()
	return applicationHeartbeat
}
