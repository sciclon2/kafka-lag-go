package main

import (
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
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

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

	// Register node
	nodeID, err := redis.RegisterNode(redisClient)
	if err != nil {
		log.Fatalf("Failed to register node: %v", err)
	}
	defer func() {
		if err := redis.DeregisterNode(redisClient, nodeID); err != nil {
			log.Printf("Failed to deregister node: %v", err)
		}
	}()

	log.Printf("Node registered with ID %d", nodeID)

	// Set the iteration interval
	interval := 30 * time.Second // default interval
	if intervalEnv := os.Getenv("ITERATION_INTERVAL"); intervalEnv != "" {
		if intervalVal, err := strconv.Atoi(intervalEnv); err == nil {
			interval = time.Duration(intervalVal) * time.Second
		}
	}

	for {
		startTime := time.Now()
		log.Println("Starting new iteration")

		// Refresh node registration
		err = redis.RefreshNode(redisClient, nodeID)
		if err != nil {
			log.Printf("Failed to refresh node registration: %v", err)
			break
		}

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
			go kafka.FetchAndDescribeGroupTopics(admin, groupChan, resultChan, &wg)
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
