package kafka

import (
	"crypto/sha256"
	"fmt"
	"regexp"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/sciclon2/kafka-lag-go/pkg/config"
	"github.com/sciclon2/kafka-lag-go/pkg/structs"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

func FetchConsumerGroups(admin KafkaAdmin, groupChan chan<- string, config *config.Config) {
	go func() {
		logrus.Debugf("Fetching consumer groups")
		consumerGroups, err := admin.ListConsumerGroups()
		if err != nil {
			logrus.Fatalf("Error listing consumer groups: %v", err)
		}

		// Prepare regex patterns from the config
		blacklist := config.Kafka.ConsumerGroups.Blacklist
		whitelist := config.Kafka.ConsumerGroups.Whitelist

		for groupID := range consumerGroups {
			// Apply filters to each consumer group
			if isGroupAllowed(groupID, blacklist, whitelist) {
				groupChan <- groupID
			}
		}
		close(groupChan)
	}()
}

// isGroupAllowed checks if a given consumer group ID is allowed based on the provided blacklist
// and whitelist filters. The function first checks the whitelist (if provided), and then the blacklist.
// A consumer group is allowed if it matches the whitelist or does not match the blacklist.
func isGroupAllowed(groupID string, blacklist *regexp.Regexp, whitelist *regexp.Regexp) bool {
	if whitelist != nil {
		if whitelist.MatchString(groupID) {
			return true
		} else {
			return false
		}
	}

	if blacklist != nil {
		if blacklist.MatchString(groupID) {
			logrus.Debugf("Group '%s' is excluded by blacklist", groupID)
			return false
		}
	}
	return true
}

func GetConsumerGroupsInfo(admin KafkaAdmin, client KafkaClient, groupNameChan <-chan string, groupStructPartialChan chan<- *structs.Group, numWorkers int, nodeIndex, totalNodes int) {
	var wg sync.WaitGroup
	client.RefreshMetadata()

	// Start multiple workers to process consumer groups concurrently
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()

			for groupID := range groupNameChan {
				logrus.Debugf("Checking if group %s should be processed by node index %d out of %d nodes", groupID, nodeIndex, totalNodes)
				if isGroupAssignedToNode(groupID, nodeIndex, totalNodes) {
					group, err := createGroupStruct(admin, groupID)
					if err != nil {
						logrus.Warnf("Error creating group struct for group %s: %v", groupID, err)
						continue
					}

					if len(group.Topics) > 0 {
						err := populateLeaderBrokerInfo(admin, client, group)
						if err != nil {
							logrus.Warnf("Error populating leader broker info for group %s: %v", groupID, err)
							continue
						}
					}

					groupStructPartialChan <- group
				}
			}
		}()
	}

	// Wait for all workers to finish and then close the channel
	go func() {
		wg.Wait()
		close(groupStructPartialChan)
	}()
}

func isGroupAssignedToNode(groupID string, nodeIndex, totalNodes int) bool {
	hash := sha256.New()
	hash.Write([]byte(groupID))
	hashSum := hash.Sum(nil)
	hashInt := int(hashSum[0])<<24 + int(hashSum[1])<<16 + int(hashSum[2])<<8 + int(hashSum[3])
	shouldProcess := hashInt%totalNodes == nodeIndex
	return shouldProcess
}

func createGroupStruct(admin KafkaAdmin, groupID string) (*structs.Group, error) {
	group := &structs.Group{
		Name:            groupID,
		Topics:          []structs.Topic{},
		MaxLagInOffsets: -1, // Initialize MaxLagInOffsets to -1
		MaxLagInSeconds: -1, // Initialize MaxLagInSeconds to -1
		SumLagInOffsets: -1, // Initialize SumLagInOffsets to -1
		SumLagInSeconds: -1, // Initialize SumLagInSeconds to -1
	}

	offsetFetchResponse, err := admin.ListConsumerGroupOffsets(groupID, nil)
	if err != nil {
		return nil, fmt.Errorf("error fetching offsets for group %s: %v", groupID, err)
	}

	for topic, partitions := range offsetFetchResponse.Blocks {
		topicStruct := structs.Topic{
			Name:            topic,
			Partitions:      []structs.Partition{},
			MaxLagInOffsets: -1, // Initialize MaxLagInOffsets to -1
			MaxLagInSeconds: -1, // Initialize MaxLagInSeconds to -1
			SumLagInOffsets: -1, // Initialize SumLagInOffsets to -1
			SumLagInSeconds: -1, // Initialize SumLagInSeconds to -1
		}

		for partition, block := range partitions {
			if block.Err == sarama.ErrNoError && block.Offset >= 0 {
				// Initialize Partition with default values
				partitionStruct := structs.Partition{
					Number:                 partition,
					CommitedOffset:         block.Offset,
					ProducedOffsetsHistory: []redis.Z{},      // Initialize as empty slice
					LatestProducedOffset:   -1,               // Initialize as -1
					LatestProducedOffsetAt: -1,               // Initialize as -1
					LeaderBroker:           &sarama.Broker{}, // Initialize as a pointer to an empty Broker
					LagInOffsets:           -1,               // Initialize as -1
					LagInSeconds:           -1,               // Initialize as -1
				}

				topicStruct.Partitions = append(topicStruct.Partitions, partitionStruct)
			}
		}

		if len(topicStruct.Partitions) > 0 {
			group.Topics = append(group.Topics, topicStruct)
		}
	}

	return group, nil
}

func populateLeaderBrokerInfo(admin KafkaAdmin, client KafkaClient, group *structs.Group) error {
	leaderMap, err := getTopicPartitionLeaders(admin, client, group.Topics)
	if err != nil {
		return fmt.Errorf("error describing topics: %v", err)
	}

	for i, topic := range group.Topics {
		for j, partition := range topic.Partitions {
			if broker, ok := leaderMap[partition.Number]; ok {
				group.Topics[i].Partitions[j].LeaderBroker = broker.(structs.BrokerInterface)
			}
		}
	}

	return nil
}
