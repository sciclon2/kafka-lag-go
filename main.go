package main

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"kafka-lag/config"
	"kafka-lag/kafka"
	"kafka-lag/redis"
	"kafka-lag/structs"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var ctx = context.Background()

func generateNodeID() string {
	return uuid.New().String()
}

func main() {
	// Load configuration
	cfg, err := config.LoadConfig("examples/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Set up Prometheus HTTP handler
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Fatal(http.ListenAndServe(":"+strconv.Itoa(cfg.Prometheus.MetricsPort), nil)) // Expose metrics on the configured port
	}()

	// Set up Sarama configuration
	kafkaConfig := sarama.NewConfig()
	kafkaVersion, err := sarama.ParseKafkaVersion(cfg.Kafka.Version)
	if err != nil {
		log.Fatalf("Error parsing Kafka version: %v", err)
	}
	kafkaConfig.Version = kafkaVersion

	// Create Kafka client and admin
	client, admin, err := kafka.CreateAdminAndClient(cfg.Kafka.Brokers, kafkaConfig)
	if err != nil {
		log.Fatalf("%v", err)
	}
	defer client.Close()
	defer admin.Close()

	// Create Redis client using the CreateRedisClient function
	redisClient := redis.CreateRedisClient(cfg.GetRedisAddress())
	defer redisClient.Close()

	// Generate a random node ID
	nodeID := generateNodeID()

	// Register node
	err = redis.RegisterNode(redisClient, nodeID, 10)
	if err != nil {
		log.Fatalf("Failed to register node: %v", err)
	}
	defer func() {
		if err := redis.DeregisterNode(redisClient, nodeID); err != nil {
			log.Printf("Failed to deregister node: %v", err)
		}
	}()

	log.Printf("Node registered with ID %s", nodeID)

	// Set the iteration interval
	iterationInterval, err := cfg.GetIterationInterval()
	if err != nil {
		log.Fatalf("Invalid iteration interval: %v", err)
	}

	// Define the internal health check intervals
	const (
		heartbeatInterval = 5 * time.Second
		monitorInterval   = 10 * time.Second
	)

	// Start goroutine for periodic Redis node refresh (heartbeats)
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			<-ticker.C
			err := redis.RefreshNode(redisClient, nodeID, 10)
			if err != nil {
				log.Printf("Failed to refresh node registration: %v", err)
			}
		}
	}()

	// Start goroutine for monitoring nodes
	go func() {
		ticker := time.NewTicker(monitorInterval)
		defer ticker.Stop()
		for {
			<-ticker.C
			failedNodes, err := redis.MonitorNodes(redisClient)
			if err != nil {
				log.Printf("Failed to monitor nodes: %v", err)
			}
			if len(failedNodes) > 0 {
				log.Printf("Removed failed nodes: %v", failedNodes)
			}
		}
	}()

	for {
		startTime := time.Now()
		log.Println("Starting new iteration")

		// Fetch active nodes
		activeNodes, err := redis.GetActiveNodes(redisClient)
		if err != nil {
			log.Printf("Failed to get active nodes: %v", err)
			break
		}

		totalNodes := len(activeNodes)
		if totalNodes == 0 {
			log.Printf("No active nodes found")
			break
		}

		// Get the index of the current node
		nodeIndex, err := redis.GetNodeIndex(nodeID, activeNodes)
		if err != nil {
			log.Printf("Failed to get node index: %v", err)
			break
		}

		log.Printf("Total nodes: %d, Current node index: %d", totalNodes, nodeIndex)

		// Channels and wait group
		groupChan := make(chan string)
		resultChan := make(chan structs.Group)
		var wgFetchAndDescribeGroupTopics sync.WaitGroup

		// Fetch consumer groups
		go kafka.FetchConsumerGroups(admin, groupChan, cfg)

		// Start worker goroutines
		numWorkers := cfg.App.NumWorkers
		if numWorkers == 0 {
			numWorkers = 5 // default number of workers
		}
		for i := 0; i < numWorkers; i++ {
			wgFetchAndDescribeGroupTopics.Add(1)
			go kafka.FetchAndDescribeGroupTopics(admin, groupChan, resultChan, &wgFetchAndDescribeGroupTopics, nodeIndex, totalNodes)
		}

		// Close result channel when all workers are done
		go func() {
			wgFetchAndDescribeGroupTopics.Wait()
			close(resultChan)
		}()

		// Start offset processing goroutines
		var wgProcessGroupOffsets sync.WaitGroup
		for i := 0; i < numWorkers; i++ {
			wgProcessGroupOffsets.Add(1)
			go kafka.ProcessGroupOffsets(client, admin, resultChan, redisClient, &wgProcessGroupOffsets, kafkaConfig)
		}
		wgProcessGroupOffsets.Wait()

		// Calculate elapsed time and adjust sleep duration
		elapsedTime := time.Since(startTime)
		if elapsedTime < iterationInterval {
			sleepDuration := iterationInterval - elapsedTime
			log.Printf("Iteration completed. Sleeping for %v until next iteration.", sleepDuration)
			time.Sleep(sleepDuration)
		} else {
			log.Println("Iteration took longer than the interval. Starting next iteration immediately.")
		}
	}
}
