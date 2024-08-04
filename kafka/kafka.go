package kafka

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"
	"github.com/go-redis/redis/v8"
	"kafka-lag/structs"
	"kafka-lag/utils"
)

var ctx = context.Background()

// CreateAdminAndClient creates a new Kafka client and admin
func CreateAdminAndClient(brokers []string, config *sarama.Config) (sarama.Client, sarama.ClusterAdmin, error) {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating Kafka client: %v", err)
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		client.Close()
		return nil, nil, fmt.Errorf("error creating Kafka admin client: %v", err)
	}

	return client, admin, nil
}

// FetchConsumerGroups fetches all consumer groups
func FetchConsumerGroups(admin sarama.ClusterAdmin, groupChan chan<- string) {
	consumerGroups, err := admin.ListConsumerGroups()
	if err != nil {
		log.Fatalf("Error listing consumer groups: %v", err)
	}

	for groupID := range consumerGroups {
		groupChan <- groupID
	}
	close(groupChan)
}

// FetchTopicsForGroup fetches topics for a consumer group
func FetchTopicsForGroup(admin sarama.ClusterAdmin, groupID string) ([]string, error) {
	offsetFetchResponse, err := admin.ListConsumerGroupOffsets(groupID, nil)
	if err != nil {
		return nil, fmt.Errorf("error fetching offsets for group %s: %v", groupID, err)
	}

	topicSet := make(map[string]struct{})
	for topicName := range offsetFetchResponse.Blocks {
		topicSet[topicName] = struct{}{}
	}

	var topics []string
	for topicName := range topicSet {
		topics = append(topics, topicName)
	}

	return topics, nil
}

// DescribeTopics describes topics and gets partitions
func DescribeTopics(admin sarama.ClusterAdmin, topics []string) ([]structs.Topic, error) {
	topicMetadata, err := admin.DescribeTopics(topics)
	if err != nil {
		return nil, fmt.Errorf("error describing topics: %v", err)
	}

	var result []structs.Topic
	for _, topic := range topicMetadata {
		topicStruct := structs.Topic{
			Name:            topic.Name,
			NumOfPartitions: len(topic.Partitions),
		}

		for _, partition := range topic.Partitions {
			topicStruct.Partitions = append(topicStruct.Partitions, structs.Partition{
				Number: partition.ID,
			})
		}

		result = append(result, topicStruct)
	}

	return result, nil
}

// FetchAndDescribeGroupTopics fetches and describes group topics
func FetchAndDescribeGroupTopics(admin sarama.ClusterAdmin, groupChan <-chan string, resultChan chan<- structs.Group, wg *sync.WaitGroup) {
	defer wg.Done()

	for groupID := range groupChan {
		var group structs.Group
		group.Name = groupID

		topics, err := FetchTopicsForGroup(admin, groupID)
		if err != nil {
			log.Printf("%v", err)
			continue
		}

		topicDetails, err := DescribeTopics(admin, topics)
		if err != nil {
			log.Printf("%v", err)
			continue
		}

		group.Topics = topicDetails
		resultChan <- group
	}
}

// FetchOffsets fetches offsets from a broker
func FetchOffsets(broker *sarama.Broker, topicName string, partitions []int32, config *sarama.Config) (map[int32]*sarama.OffsetResponseBlock, error) {
	request := &sarama.OffsetRequest{}
	for _, partition := range partitions {
		request.AddBlock(topicName, partition, sarama.OffsetNewest, 1)
	}

	// Open broker connection if not already open
	err := broker.Open(config)
	if err != nil && err != sarama.ErrAlreadyConnected {
		return nil, fmt.Errorf("error opening broker connection: %v", err)
	}

	response, err := broker.GetAvailableOffsets(request)
	if err != nil {
		return nil, fmt.Errorf("error listing offsets for broker %s: %w", broker.Addr(), err)
	}

	return response.Blocks[topicName], nil
}

// StoreOffsetsInRedis stores offsets in Redis
func StoreOffsetsInRedis(pipe redis.Pipeliner, ctx context.Context, topicName string, offsets map[int32]*sarama.OffsetResponseBlock) {
	utils.StoreOffsetsInRedis(pipe, ctx, topicName, offsets)
}

// RefreshMetadata refreshes metadata for a topic
func RefreshMetadata(client sarama.Client, topicName string) error {
	return client.RefreshMetadata(topicName)
}

// GroupPartitionsByBroker groups partitions by their leader broker
func GroupPartitionsByBroker(client sarama.Client, topic structs.Topic) (map[*sarama.Broker][]int32, error) {
	partitions, err := client.Partitions(topic.Name)
	if err != nil {
		return nil, fmt.Errorf("error getting partitions for topic %s: %w", topic.Name, err)
	}

	brokerPartitions := make(map[*sarama.Broker][]int32)
	for _, partition := range partitions {
		broker, err := client.Leader(topic.Name, partition)
		if err != nil {
			return nil, fmt.Errorf("error getting leader for topic %s partition %d: %w", topic.Name, partition, err)
		}
		brokerPartitions[broker] = append(brokerPartitions[broker], partition)
	}
	return brokerPartitions, nil
}

// ProcessGroupOffsets processes group offsets
func ProcessGroupOffsets(client sarama.Client, admin sarama.ClusterAdmin, groupChan <-chan structs.Group, redisClient *redis.Client, wg *sync.WaitGroup, config *sarama.Config) {
	defer wg.Done()

	for group := range groupChan {
		var innerWg sync.WaitGroup
		innerWg.Add(2)

		// Goroutine to fetch produced offsets
		go fetchProducedOffsets(client, group, redisClient, &innerWg, config)

		// Goroutine to fetch committed offsets
		go fetchCommittedOffsets(admin, group, &innerWg)

		innerWg.Wait()
	}
}

func fetchProducedOffsets(client sarama.Client, group structs.Group, redisClient *redis.Client, wg *sync.WaitGroup, config *sarama.Config) {
	defer wg.Done()

	pipe := redisClient.Pipeline() // Start a pipeline

	for _, topic := range group.Topics {
		// Refresh metadata to ensure we have the latest information
		err := RefreshMetadata(client, topic.Name)
		if err != nil {
			log.Printf("Error refreshing metadata for topic %s: %v", topic.Name, err)
			continue
		}

		// Group partitions by their leader broker
		brokerPartitions, err := GroupPartitionsByBroker(client, topic)
		if err != nil {
			log.Printf("%v", err)
			continue
		}

		// For each broker, prepare and send an OffsetRequest for the partitions it leads
		for broker, partitions := range brokerPartitions {
			offsets, err := FetchOffsets(broker, topic.Name, partitions, config)
			if err != nil {
				log.Printf("Error fetching offsets for broker %s, topic %s: %v", broker.Addr(), topic.Name, err)
				continue
			}

			// Store the offsets in Redis using the pipeline
			StoreOffsetsInRedis(pipe, ctx, topic.Name, offsets)
		}
	}

	results, err := pipe.Exec(ctx)
	if err != nil {
		log.Printf("Error executing Redis pipeline: %v", err)
		for _, cmd := range results {
			if cmd.Err() != nil {
				log.Printf("Pipeline command error: %v", cmd.Err())
			} else {
				log.Printf("Pipeline command result: %v", cmd)
			}
		}
	}
}

func fetchCommittedOffsets(admin sarama.ClusterAdmin, group structs.Group, wg *sync.WaitGroup) {
	defer wg.Done()
	for _, topic := range group.Topics {
		partitions := make([]int32, len(topic.Partitions))
		for i, partition := range topic.Partitions {
			partitions[i] = partition.Number
		}
		offsets, err := admin.ListConsumerGroupOffsets(group.Name, map[string][]int32{topic.Name: partitions})
		if err != nil {
			log.Printf("Error fetching committed offsets for group %s: %v", group.Name, err)
			continue
		}

		for partition, block := range offsets.Blocks[topic.Name] {
			if block.Err != sarama.ErrNoError {
				log.Printf("Error fetching committed offset for topic %s partition %d: %v", topic.Name, partition, block.Err)
				continue
			}
			fmt.Printf("Committed Offset - Group: %s, Topic: %s, Partition: %d, Offset: %d\n", group.Name, topic.Name, partition, block.Offset)
		}
	}
}
