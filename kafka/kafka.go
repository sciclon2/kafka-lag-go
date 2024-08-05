package kafka

import (
	"context"
	"crypto/sha256"
	"fmt"
	"kafka-lag/structs"
	"kafka-lag/utils"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
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
	log.Println("Fetching consumer groups")
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
func FetchAndDescribeGroupTopics(admin sarama.ClusterAdmin, groupChan <-chan string, resultChan chan<- structs.Group, wg *sync.WaitGroup, nodeIndex, totalNodes int) {
	defer wg.Done()

	for groupID := range groupChan {
		log.Printf("Checking if group %s should be processed by node index %d out of %d nodes", groupID, nodeIndex, totalNodes)
		if shouldProcessGroup(groupID, nodeIndex, totalNodes) {
			var group structs.Group
			group.Name = groupID

			topics, err := FetchTopicsForGroup(admin, groupID)
			if err != nil {
				log.Printf("Error fetching topics for group %s: %v", groupID, err)
				continue
			}

			log.Printf("Fetched topics for group %s: %v", groupID, topics)
			topicDetails, err := DescribeTopics(admin, topics)
			if err != nil {
				log.Printf("Error describing topics for group %s: %v", groupID, err)
				continue
			}

			group.Topics = topicDetails
			resultChan <- group
		}
	}
}

// shouldProcessGroup determines if the current node should process the given groupID
func shouldProcessGroup(groupID string, nodeIndex, totalNodes int) bool {
	hash := sha256.New()
	hash.Write([]byte(groupID))
	hashSum := hash.Sum(nil)
	hashInt := int(hashSum[0])<<24 + int(hashSum[1])<<16 + int(hashSum[2])<<8 + int(hashSum[3])
	shouldProcess := hashInt%totalNodes == nodeIndex
	log.Printf("Group %s hash %d, should process: %v", groupID, hashInt%totalNodes, shouldProcess)
	return shouldProcess
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

// ProcessGroupOffsets processes group offsets and calculates lag
func ProcessGroupOffsets(client sarama.Client, admin sarama.ClusterAdmin, groupChan <-chan structs.Group, redisClient *redis.Client, wg *sync.WaitGroup, config *sarama.Config) {
	defer wg.Done()

	for group := range groupChan {
		var innerWg sync.WaitGroup
		producedOffsetsChan := make(chan map[string]map[int32]int64, 1)
		committedOffsetsChan := make(chan map[string]map[int32]int64, 1)

		// Goroutine to fetch produced offsets
		innerWg.Add(1)
		go func() {
			defer innerWg.Done()
			producedOffsets := fetchProducedOffsets(client, group, redisClient, config)
			producedOffsetsChan <- producedOffsets
		}()

		// Goroutine to fetch committed offsets
		innerWg.Add(1)
		go func() {
			defer innerWg.Done()
			committedOffsets := fetchCommittedOffsets(admin, group)
			committedOffsetsChan <- committedOffsets
		}()
		innerWg.Wait()
		close(producedOffsetsChan)
		close(committedOffsetsChan)

		producedOffsets := <-producedOffsetsChan
		committedOffsets := <-committedOffsetsChan

		// Calculate lag
		calculateLag(group.Name, producedOffsets, committedOffsets, redisClient)
	}
}

func fetchProducedOffsets(client sarama.Client, group structs.Group, redisClient *redis.Client, config *sarama.Config) map[string]map[int32]int64 {
	pipe := redisClient.Pipeline()
	producedOffsets := make(map[string]map[int32]int64)

	for _, topic := range group.Topics {
		RefreshMetadata(client, topic.Name)
		brokerPartitions, _ := GroupPartitionsByBroker(client, topic)

		for broker, partitions := range brokerPartitions {
			offsets, _ := FetchOffsets(broker, topic.Name, partitions, config)
			for partition, block := range offsets {
				if block.Err == sarama.ErrNoError {
					if producedOffsets[topic.Name] == nil {
						producedOffsets[topic.Name] = make(map[int32]int64)
					}
					producedOffsets[topic.Name][partition] = block.Offsets[0]
				}
			}
			// Store the offsets in Redis using the pipeline
			StoreOffsetsInRedis(pipe, ctx, topic.Name, offsets)
		}
	}

	pipe.Exec(ctx)
	return producedOffsets
}

func fetchCommittedOffsets(admin sarama.ClusterAdmin, group structs.Group) map[string]map[int32]int64 {
	committedOffsets := make(map[string]map[int32]int64)

	for _, topic := range group.Topics {
		partitions := make([]int32, len(topic.Partitions))
		for i, partition := range topic.Partitions {
			partitions[i] = partition.Number
		}
		offsets, _ := admin.ListConsumerGroupOffsets(group.Name, map[string][]int32{topic.Name: partitions})
		for partition, block := range offsets.Blocks[topic.Name] {
			if block.Err == sarama.ErrNoError {
				if committedOffsets[topic.Name] == nil {
					committedOffsets[topic.Name] = make(map[int32]int64)
				}
				committedOffsets[topic.Name][partition] = block.Offset
			}
		}
	}

	return committedOffsets
}

func calculateLag(groupName string, producedOffsets, committedOffsets map[string]map[int32]int64, redisClient *redis.Client) {
	currentTime := time.Now().UnixNano() / int64(time.Millisecond)

	for topic, partitions := range producedOffsets {
		for partition, producedOffset := range partitions {
			committedOffset := committedOffsets[topic][partition]
			lag := producedOffset - committedOffset
			log.Printf("Offset Lag - Group: %s, Topic: %s, Partition: %d, Offset Lag: %d", groupName, topic, partition, lag)

			// Fetch the interpolation table from Redis
			interpolationData := fetchInterpolationData(redisClient, topic, partition)
			if len(interpolationData) < 2 {
				log.Printf("Not enough data to calculate time lag for Group: %s, Topic: %s, Partition: %d", groupName, topic, partition)
				continue
			}

			log.Printf("Interpolation Data for Group: %s, Topic: %s, Partition: %d: %+v", groupName, topic, partition, interpolationData)

			// Calculate the production timestamp for the last consumed offset
			predictedTimestamp := interpolateTimestamp(committedOffset, interpolationData)
			log.Printf("Predicted Timestamp for Group: %s, Topic: %s, Partition: %d, Committed Offset: %d, Predicted Timestamp: %d", groupName, topic, partition, committedOffset, predictedTimestamp)

			// Calculate time lag
			timeLag := currentTime - predictedTimestamp

			log.Printf("Time Lag (seconds) - Group: %s, Topic: %s, Partition: %d, Time Lag: %d", groupName, topic, partition, timeLag/1000)

			// Record the lag in Prometheus
			KafkaLagOffset.With(prometheus.Labels{
				"group":     groupName,
				"topic":     topic,
				"partition": strconv.Itoa(int(partition)),
			}).Set(float64(lag))

			KafkaTimeLag.With(prometheus.Labels{
				"group":     groupName,
				"topic":     topic,
				"partition": strconv.Itoa(int(partition)),
			}).Set(float64(timeLag) / 1000.0) // Time lag in seconds
		}
	}
}

func interpolateTimestamp(consumedOffset int64, interpolationTable []structs.OffsetTimestamp) int64 {
	var x1, x2, y1, y2 int64

	for i := 0; i < len(interpolationTable)-1; i++ {
		if interpolationTable[i].Offset <= consumedOffset && consumedOffset <= interpolationTable[i+1].Offset {
			x1, y1 = interpolationTable[i].Timestamp, interpolationTable[i].Offset
			x2, y2 = interpolationTable[i+1].Timestamp, interpolationTable[i+1].Offset
			if y2 == y1 {
				log.Printf("Warning: Offsets for interpolation points are identical, using latest timestamp x2: %d", interpolationTable[i+1].Timestamp)
				return interpolationTable[i+1].Timestamp
			}
			log.Printf("Interpolating between points: (%d, %d) and (%d, %d) for offset %d", y1, x1, y2, x2, consumedOffset)
			return x2 - (y2-consumedOffset)*(x2-x1)/(y2-y1)
		}
	}

	// If the loop didn't return, check if the last two entries have the same offset
	if len(interpolationTable) > 1 && interpolationTable[len(interpolationTable)-1].Offset == interpolationTable[len(interpolationTable)-2].Offset {
		log.Printf("Using latest timestamp for repeated offset: %d", interpolationTable[len(interpolationTable)-1].Timestamp)
		return interpolationTable[len(interpolationTable)-1].Timestamp
	}

	// If no points found for interpolation, use extrapolation with the first and last points
	x1, y1 = interpolationTable[0].Timestamp, interpolationTable[0].Offset
	x2, y2 = interpolationTable[len(interpolationTable)-1].Timestamp, interpolationTable[len(interpolationTable)-1].Offset
	if y2 == y1 {
		log.Printf("Warning: Offsets for extrapolation points are identical, using latest timestamp x2: %d", interpolationTable[len(interpolationTable)-1].Timestamp)
		return interpolationTable[len(interpolationTable)-1].Timestamp
	}
	log.Printf("Extrapolating between points: (%d, %d) and (%d, %d) for offset %d", y1, x1, y2, x2, consumedOffset)
	return x2 - (y2-consumedOffset)*(x2-x1)/(y2-y1)
}

func fetchInterpolationData(redisClient *redis.Client, topic string, partition int32) []structs.OffsetTimestamp {
	key := fmt.Sprintf("%s:%d", topic, partition)
	data, err := redisClient.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		log.Printf("Error fetching interpolation data from Redis for %s:%d: %v", topic, partition, err)
		return nil
	}

	log.Printf("Fetched raw data from Redis for %s:%d: %+v", topic, partition, data)

	// Use a map to store the latest timestamp for each offset
	offsetMap := make(map[int64]structs.OffsetTimestamp)
	for _, item := range data {
		timestampStr := item.Member.(string)
		offset := int64(item.Score)

		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			log.Printf("Error parsing timestamp: %v", err)
			continue
		}

		// Update the map to keep the latest timestamp for each offset
		if existing, found := offsetMap[offset]; !found || existing.Timestamp < timestamp {
			offsetMap[offset] = structs.OffsetTimestamp{
				Timestamp: timestamp,
				Offset:    offset,
			}
		}
	}

	// Convert the map back to a sorted slice
	var interpolationTable []structs.OffsetTimestamp
	for _, v := range offsetMap {
		interpolationTable = append(interpolationTable, v)
	}

	// Sort the slice by offset
	sort.Slice(interpolationTable, func(i, j int) bool {
		return interpolationTable[i].Offset < interpolationTable[j].Offset
	})

	log.Printf("Final interpolation data for %s:%d - %+v", topic, partition, interpolationTable)
	return interpolationTable
}
