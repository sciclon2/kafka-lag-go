package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

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
	// Set up Prometheus HTTP handler
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Fatal(http.ListenAndServe(":9090", nil)) // Expose metrics on port 9090
	}()

	// Set up Sarama configuration
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0 // specify appropriate Kafka version

	// Read broker addresses from environment variables or use default
	brokers := []string{"localhost:29092"} // update with your broker addresses
	if brokersEnv := os.Getenv("KAFKA_BROKERS"); brokersEnv != "" {
		brokers = strings.Split(brokersEnv, ",")
	}

	// Create Kafka client and admin
	client, admin, err := kafka.CreateAdminAndClient(brokers, config)
	if err != nil {
		log.Fatalf("%v", err)
	}
	defer client.Close()
	defer admin.Close()

	// Create Redis client
	redisClient := redis.CreateRedisClient()
	defer redisClient.Close()

	// Generate a random node ID
	nodeID := generateNodeID()

	// Register node
	err = redis.RegisterNode(redisClient, nodeID)
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
	interval := 30 * time.Second // default interval
	if intervalEnv := os.Getenv("ITERATION_INTERVAL"); intervalEnv != "" {
		if intervalVal, err := strconv.Atoi(intervalEnv); err == nil {
			interval = time.Duration(intervalVal) * time.Second
		}
	}

	// Start goroutine for periodic Redis node refresh (heartbeats)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := redis.RefreshNode(redisClient, nodeID); err != nil {
					log.Printf("Failed to refresh node registration: %v", err)
				}
			}
		}
	}()

	// Start goroutine for monitoring nodes
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				failedNodes, err := redis.MonitorNodes(redisClient)
				if err != nil {
					log.Printf("Failed to monitor nodes: %v", err)
				}
				if len(failedNodes) > 0 {
					log.Printf("Removed failed nodes: %v", failedNodes)
				}
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
		var wg sync.WaitGroup

		// Fetch consumer groups
		go kafka.FetchConsumerGroups(admin, groupChan)

		// Start worker goroutines
		numWorkers, _ := strconv.Atoi(os.Getenv("NUM_WORKERS"))
		if numWorkers == 0 {
			numWorkers = 5 // default number of workers
		}
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go kafka.FetchAndDescribeGroupTopics(admin, groupChan, resultChan, &wg, nodeIndex, totalNodes)
		}

		// Close result channel when all workers are done
		go func() {
			wg.Wait()
			close(resultChan)
		}()

		// Start offset processing goroutines
		var wg2 sync.WaitGroup
		for i := 0; i < numWorkers; i++ {
			wg2.Add(1)
			go kafka.ProcessGroupOffsets(client, admin, resultChan, redisClient, &wg2, config)
		}
		wg2.Wait()

		// Calculate elapsed time and adjust sleep duration
		elapsedTime := time.Since(startTime)
		if elapsedTime < interval {
			sleepDuration := interval - elapsedTime
			log.Printf("Iteration completed. Sleeping for %v until next iteration.", sleepDuration)
			time.Sleep(sleepDuration)
		} else {
			log.Println("Iteration took longer than the interval. Starting next iteration immediately.")
		}
	}
}
