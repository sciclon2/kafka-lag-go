package kafka

import (
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/sciclon2/kafka-lag-go/pkg/structs"
	"github.com/sirupsen/logrus"
)

func GetLatestProducedOffsets(groupsWithLeaderInfoChan <-chan *structs.Group, numWorkers int) <-chan *structs.Group {
	var wg sync.WaitGroup
	groupsWithLeaderInfoAndLeaderOffsetsChan := make(chan *structs.Group)

	// Start multiple workers to process group offsets concurrently
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()

			for group := range groupsWithLeaderInfoChan {
				processGroupOffsets(group, numWorkers)
				groupsWithLeaderInfoAndLeaderOffsetsChan <- group
			}
		}()
	}

	// Wait for all workers to finish and then close the channel
	go func() {
		wg.Wait()
		close(groupsWithLeaderInfoAndLeaderOffsetsChan)
	}()
	return groupsWithLeaderInfoAndLeaderOffsetsChan
}

func processGroupOffsets(group *structs.Group, numWorkers int) {
	// Group partitions by their leader broker
	brokerPartitionMap := groupPartitionsByBroker(group)

	var wg sync.WaitGroup
	offsetResults := make(chan map[string]map[int32]int64, len(brokerPartitionMap))
	sem := make(chan struct{}, numWorkers) // Semaphore to limit concurrent goroutines

	for broker, topicPartitions := range brokerPartitionMap {
		sem <- struct{}{} // Acquire a slot in the semaphore
		wg.Add(1)
		go func(broker *sarama.Broker, topicPartitions map[string][]structs.Partition) {
			defer wg.Done()
			defer func() { <-sem }() // Release the slot in the semaphore

			// Fetch offsets using the saramaConfig from the group struct
			offsets, err := fetchLatestProducedOffsetsFromBroker(broker, topicPartitions, group.SaramaConfig)
			if err != nil {
				logrus.Warnf("Error fetching produced offsets from broker %s: %v", broker.Addr(), err)
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

func groupPartitionsByBroker(group *structs.Group) map[*sarama.Broker]map[string][]structs.Partition {
	brokerPartitionMap := make(map[*sarama.Broker]map[string][]structs.Partition)

	for _, topic := range group.Topics {
		for _, partition := range topic.Partitions {
			if partition.LeaderBroker != nil {
				leaderBroker := partition.LeaderBroker // Directly use the *sarama.Broker

				if brokerPartitionMap[leaderBroker] == nil {
					brokerPartitionMap[leaderBroker] = make(map[string][]structs.Partition)
				}
				brokerPartitionMap[leaderBroker][topic.Name] = append(brokerPartitionMap[leaderBroker][topic.Name], partition)
			}
		}
	}

	return brokerPartitionMap
}

// Function to process the OffsetResponse and extract offsets into a structured map
func fetchLatestProducedOffsetsFromBroker(broker *sarama.Broker, topicPartitions map[string][]structs.Partition, config *sarama.Config) (map[string]map[int32]int64, error) {
	offsetResponse, err := getOffsetResponseFromBroker(broker, topicPartitions, config)
	if err != nil {
		return nil, err
	}

	offsets := make(map[string]map[int32]int64)
	for topic, partitions := range topicPartitions {
		for _, partition := range partitions {
			if block := offsetResponse.GetBlock(topic, partition.Number); block != nil && block.Err == sarama.ErrNoError {
				// Check if there are any offsets before accessing the slice
				if len(block.Offsets) > 0 {
					if offsets[topic] == nil {
						offsets[topic] = make(map[int32]int64)
					}
					offsets[topic][partition.Number] = block.Offsets[0]
				} else {
					// Handle the case where no offsets are available
					logrus.Warnf("No offsets available for topic %s partition %d from broker %s", topic, partition.Number, broker.Addr())
				}
			}
		}
	}
	return offsets, nil
}

// Function to build and send the OffsetRequest to the broker, and fetch the OffsetResponse
func getOffsetResponseFromBroker(broker *sarama.Broker, topicPartitions map[string][]structs.Partition, config *sarama.Config) (*sarama.OffsetResponse, error) {
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
		return nil, fmt.Errorf("error fetching produced offsets from broker %s: %w", broker.Addr(), err)
	}

	logrus.Debugf("Latest Produced OffsetResponse from broker %s: %+v\n", broker.Addr(), offsetResponse)
	return offsetResponse, nil
}

func ensureBrokerConnection(broker *sarama.Broker, config *sarama.Config) error {
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
