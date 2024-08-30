package kafka

import (
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/sciclon2/kafka-lag-go/pkg/structs"
	"github.com/sirupsen/logrus"
)

func GetLatestProducedOffsets(admin KafkaAdmin, groupStructPartialChan <-chan *structs.Group, groupStructCompleteChan chan<- *structs.Group, numWorkers int, config *sarama.Config) {
	var wg sync.WaitGroup

	// Start multiple workers to process group offsets concurrently
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()

			for group := range groupStructPartialChan {
				processGroupOffsets(group, config, numWorkers)
				groupStructCompleteChan <- group
			}
		}()
	}

	// Wait for all workers to finish and then close the channel
	go func() {
		wg.Wait()
		close(groupStructCompleteChan)
	}()
}

func processGroupOffsets(group *structs.Group, config *sarama.Config, numWorkers int) {
	brokerPartitionMap := groupPartitionsByBroker(group)

	var wg sync.WaitGroup
	offsetResults := make(chan map[string]map[int32]int64, len(brokerPartitionMap))
	sem := make(chan struct{}, numWorkers) // Semaphore to limit concurrent goroutines

	for broker, topicPartitions := range brokerPartitionMap {
		sem <- struct{}{} // Acquire a slot in the semaphore
		wg.Add(1)
		go func(broker structs.BrokerInterface, topicPartitions map[string][]structs.Partition) {
			defer wg.Done()
			defer func() { <-sem }() // Release the slot in the semaphore

			offsets, err := fetchLatestProducedOffsetsFromBroker(broker, topicPartitions, config)
			if err != nil {
				logrus.Warnf("Error fetching offsets from broker %s: %v", broker.Addr(), err)
				return
			}

			offsetResults <- offsets

		}(broker, topicPartitions)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	close(offsetResults)

	// Calculate the current timestamp once for this batch of offset results
	timestampMillis := time.Now().UnixNano() / int64(time.Millisecond)

	// Iterate over the collected offset results from all brokers and update the group's partition information
	for offsetResult := range offsetResults {
		for topicName, partitionOffsets := range offsetResult {
			// Find the corresponding topic in the group's topics
			for topicIdx, topic := range group.Topics {
				if topic.Name == topicName {
					// Update the offsets for each partition within the topic
					for partitionIdx := range topic.Partitions {
						partition := &topic.Partitions[partitionIdx]
						if offset, exists := partitionOffsets[partition.Number]; exists {
							group.Topics[topicIdx].Partitions[partitionIdx].LatestProducedOffset = offset
							group.Topics[topicIdx].Partitions[partitionIdx].LatestProducedOffsetAt = timestampMillis
						}
					}
				}
			}
		}
	}
}

func groupPartitionsByBroker(group *structs.Group) map[structs.BrokerInterface]map[string][]structs.Partition {
	brokerPartitionMap := make(map[structs.BrokerInterface]map[string][]structs.Partition)

	for _, topic := range group.Topics {
		for _, partition := range topic.Partitions {
			if partition.LeaderBroker != nil {
				if brokerPartitionMap[partition.LeaderBroker] == nil {
					brokerPartitionMap[partition.LeaderBroker] = make(map[string][]structs.Partition)
				}
				brokerPartitionMap[partition.LeaderBroker][topic.Name] = append(brokerPartitionMap[partition.LeaderBroker][topic.Name], partition)
			}
		}
	}

	return brokerPartitionMap
}

// Function to process the OffsetResponse and extract offsets into a structured map
func fetchLatestProducedOffsetsFromBroker(broker structs.BrokerInterface, topicPartitions map[string][]structs.Partition, config *sarama.Config) (map[string]map[int32]int64, error) { // Use BrokerInterface
	offsetResponse, err := getOffsetResponseFromBroker(broker, topicPartitions, config)
	if err != nil {
		return nil, err
	}

	offsets := make(map[string]map[int32]int64)
	for topic, partitions := range topicPartitions {
		for _, partition := range partitions {
			if block := offsetResponse.GetBlock(topic, partition.Number); block != nil && block.Err == sarama.ErrNoError {
				if offsets[topic] == nil {
					offsets[topic] = make(map[int32]int64)
				}
				offsets[topic][partition.Number] = block.Offsets[0]
			}
		}
	}
	return offsets, nil
}

// Function to build and send the OffsetRequest to the broker, and fetch the OffsetResponse
func getOffsetResponseFromBroker(broker structs.BrokerInterface, topicPartitions map[string][]structs.Partition, config *sarama.Config) (*sarama.OffsetResponse, error) {
	offsetRequest := &sarama.OffsetRequest{}
	for topic, partitions := range topicPartitions {
		for _, partition := range partitions {
			offsetRequest.AddBlock(topic, partition.Number, sarama.OffsetNewest, 1)
		}
	}

	if err := ensureBrokerConnection(broker, config); err != nil {
		return nil, err
	}

	offsetResponse, err := broker.GetAvailableOffsets(offsetRequest)
	if err != nil {
		return nil, fmt.Errorf("error fetching offsets from broker %s: %w", broker.Addr(), err)
	}

	logrus.Debugf("OffsetResponse from broker %s: %+v\n", broker.Addr(), offsetResponse)
	return offsetResponse, nil
}

func ensureBrokerConnection(broker structs.BrokerInterface, config *sarama.Config) error {
	connected, err := broker.Connected()
	if err != nil {
		return fmt.Errorf("error checking broker connection state: %w", err)
	}
	if !connected {
		if err := broker.Open(config); err != nil && err != sarama.ErrAlreadyConnected {
			return fmt.Errorf("error connecting to broker %s: %w", broker.Addr(), err)
		}
	}
	return nil
}

// getTopicPartitionLeaders retrieves the leader brokers for each partition of the specified topics
// in a Kafka consumer group. It does this by describing the topics and mapping each partition's leader
// to the corresponding broker.
func getTopicPartitionLeaders(admin KafkaAdmin, client KafkaClient, topics []structs.Topic) (map[int32]structs.BrokerInterface, error) {
	brokerMap := make(map[int32]structs.BrokerInterface)
	for _, broker := range client.Brokers() {
		brokerMap[broker.ID()] = broker
	}

	leaderMap := make(map[int32]structs.BrokerInterface)
	topicNames := make([]string, len(topics))
	for i, topic := range topics {
		topicNames[i] = topic.Name
	}

	topicMetadata, err := admin.DescribeTopics(topicNames)
	if err != nil {
		return nil, fmt.Errorf("error describing topics: %v", err)
	}

	for _, topic := range topicMetadata {
		for _, partition := range topic.Partitions {
			leaderMap[partition.ID] = brokerMap[partition.Leader]
		}
	}

	return leaderMap, nil
}
