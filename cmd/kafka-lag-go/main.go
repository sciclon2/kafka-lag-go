package main

import (
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/sciclon2/kafka-lag-go/pkg/config"
	"github.com/sciclon2/kafka-lag-go/pkg/heartbeat"
	"github.com/sciclon2/kafka-lag-go/pkg/kafka"
	"github.com/sciclon2/kafka-lag-go/pkg/metrics"
	"github.com/sciclon2/kafka-lag-go/pkg/storage"
	"github.com/sciclon2/kafka-lag-go/pkg/structs"

	_ "net/http/pprof"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	// Get the configuration file path using the helper function.
	configPath := config.GetConfigFilePath()

	// Load configuration from the specified YAML file.
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Set up logging
	cfg.SetLogLevel()

	// Set up Sarama (Kafka client) configuration.
	saramaConfig := sarama.NewConfig()
	client, admin, err := kafka.CreateAdminAndClient(cfg, saramaConfig)
	if err != nil {
		logrus.Fatalf("%v", err)
	}
	defer client.Close()
	defer admin.Close()

	// Set up and start Prometheus metrics HTTP server.
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		logrus.Fatal(http.ListenAndServe(":"+strconv.Itoa(cfg.Prometheus.MetricsPort), nil))
	}()

	// Initialize the Storage interface using the function from the storage package
	store, err := storage.InitializeStorage(cfg)
	if err != nil {
		logrus.Fatalf("Failed to initialize storage: %v\n", err)
	}
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

		SleepToMaintainInterval(startTime, iterationInterval)
	}
}

// SleepToMaintainInterval calculates the elapsed time for an iteration and sleeps if necessary to maintain the configured interval.
// It returns the elapsed time for logging or further processing.
func SleepToMaintainInterval(startTime time.Time, iterationInterval time.Duration) time.Duration {
	elapsedTime := time.Since(startTime)

	// Determine if sleep is needed to maintain the iteration interval.
	if elapsedTime < iterationInterval {
		sleepDuration := iterationInterval - elapsedTime
		logrus.Debugf("Iteration completed. Sleeping for %v until next iteration.", sleepDuration)
		time.Sleep(sleepDuration) // Sleep to maintain the configured iteration interval.
	} else {
		logrus.Debugf("Iteration took longer than the interval. Starting next iteration immediately.")
	}

	// Return the elapsed time for potential further use.
	return elapsedTime
}

// initializeAndStartHeartbeat initializes the ApplicationHeartbeat and starts the health check routine.
func initializeAndStartHeartbeat(kafkaAdmin kafka.KafkaAdmin, store storage.Storage, interval time.Duration, cfg *config.Config) *heartbeat.ApplicationHeartbeat {
	applicationHeartbeat := heartbeat.NewApplicationHeartbeat(kafkaAdmin, store, interval, cfg.App.HealthCheckPort, cfg.App.HealthCheckPath)
	applicationHeartbeat.Start()
	return applicationHeartbeat
}
