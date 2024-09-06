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

func GetConsumerGroupsInfo(
	groupsChan <-chan *structs.Group,
	numWorkers int,
	nodeIndex, totalNodes int) <-chan *structs.Group {

	var wg sync.WaitGroup
	groupsWithLeaderInfoChan := make(chan *structs.Group)

	// Start multiple workers to process consumer groups concurrently
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()

			for group := range groupsChan {
				logrus.Debugf("Checking if group %s should be processed by node index %d out of %d nodes", group.Name, nodeIndex, totalNodes)
				if isGroupAssignedToNode(group.Name, nodeIndex, totalNodes) {
					// Refresh metadata before processing the group
					if err := group.Client.RefreshMetadata(); err != nil {
						logrus.Warnf("Error refreshing metadata for group %s: %v", group.Name, err)
					}

					if len(group.Topics) > 0 {
						err := populateLeaderBrokerInfo(group)
						if err != nil {
							logrus.Warnf("Error populating leader broker info for group %s: %v", group.Name, err)
							continue
						}
					}
					groupsWithLeaderInfoChan <- group
				}
			}
		}()
	}

	// Close the channel after all workers are done
	go func() {
		wg.Wait()
		close(groupsWithLeaderInfoChan)
	}()

	return groupsWithLeaderInfoChan
}

func AssembleGroups(
	clientMap map[string]structs.KafkaClient,
	adminMap map[string]structs.KafkaAdmin,
	saramaConfigMap map[string]*sarama.Config,
	cfg *config.Config) <-chan *structs.Group {

	groupsChan := make(chan *structs.Group)
	var wg sync.WaitGroup

	// Iterate over each Kafka cluster (admin, client, and saramaConfig)
	for clusterName, admin := range adminMap {
		client, ok := clientMap[clusterName]
		if !ok {
			logrus.Warnf("No client found for cluster: %s", clusterName)
			continue
		}

		saramaConfig, ok := saramaConfigMap[clusterName]
		if !ok {
			logrus.Warnf("No sarama config found for cluster: %s", clusterName)
			continue
		}

		wg.Add(1)
		go func(clusterName string, admin structs.KafkaAdmin, client structs.KafkaClient, saramaConfig *sarama.Config) {
			defer wg.Done()
			// Fetch the list of consumer groups for this cluster
			groupIDs, err := admin.ListConsumerGroups()
			if err != nil {
				logrus.Errorf("Failed to list consumer groups for cluster %s: %v", clusterName, err)
				return
			}
			// Create Group structs for each consumer group
			for groupID := range groupIDs {
				// Retrieve the specific cluster configuration
				clusterConfig, err := cfg.GetClusterConfig(clusterName)
				if err != nil {
					logrus.Warnf("Error retrieving configuration for cluster %s: %v", clusterName, err)
					continue
				}
				if isGroupAllowed(groupID, clusterConfig.ConsumerGroups.Blacklist, clusterConfig.ConsumerGroups.Whitelist) {
					group, err := createGroupStruct(clusterName, admin, client, saramaConfig, groupID)
					if err != nil {
						logrus.Warnf("Error creating group struct for group %s in cluster %s: %v", groupID, clusterName, err)
						continue
					}

					// Send the group struct to the channel
					groupsChan <- group
				} else {
					logrus.Debugf("Group %s in cluster %s did not pass the whitelist/blacklist filter", groupID, clusterName)
				}

			}
		}(clusterName, admin, client, saramaConfig)
	}

	// Close the channel once all goroutines have completed
	go func() {
		wg.Wait()
		close(groupsChan)
	}()

	return groupsChan
}

func isGroupAssignedToNode(groupID string, nodeIndex, totalNodes int) bool {
	hash := sha256.New()
	hash.Write([]byte(groupID))
	hashSum := hash.Sum(nil)
	hashInt := int(hashSum[0])<<24 + int(hashSum[1])<<16 + int(hashSum[2])<<8 + int(hashSum[3])
	shouldProcess := hashInt%totalNodes == nodeIndex
	return shouldProcess
}

// func createGroupStruct(admin KafkaAdmin, groupID string) (*structs.Group, error) {
func createGroupStruct(
	clusterName string,
	admin structs.KafkaAdmin,
	client structs.KafkaClient,
	saramaConfig *sarama.Config,
	groupID string) (*structs.Group, error) {

	group := &structs.Group{
		Name:            groupID,
		ClusterName:     clusterName,
		Admin:           admin,
		Client:          client,
		SaramaConfig:    saramaConfig,
		Topics:          []structs.Topic{},
		MaxLagInOffsets: -1, // Initialize MaxLagInOffsets to -1
		MaxLagInSeconds: -1, // Initialize MaxLagInSeconds to -1
		SumLagInOffsets: -1, // Initialize SumLagInOffsets to -1
		SumLagInSeconds: -1, // Initialize SumLagInSeconds to -1
	}

	// Fetch the offsets for the consumer group
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
					ProducedOffsetsHistory: []redis.Z{}, // Initialize as empty slice
					LatestProducedOffset:   -1,          // Initialize as -1
					LatestProducedOffsetAt: -1,          // Initialize as -1
					LeaderBroker:           nil,         // Initialize as nil, to be assigned later
					LagInOffsets:           -1,          // Initialize as -1
					LagInSeconds:           -1,          // Initialize as -1
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

func populateLeaderBrokerInfo(group *structs.Group) error {
	leaderMap, err := getTopicPartitionLeaders(group)
	if err != nil {
		return fmt.Errorf("error describing topics: %v", err)
	}

	for i, topic := range group.Topics {
		for j, partition := range topic.Partitions {
			if broker, ok := leaderMap[partition.Number]; ok {
				group.Topics[i].Partitions[j].LeaderBroker = broker
			} else {
				fmt.Printf("No Broker assigned to Partition %d\n", partition.Number)
			}
		}
	}

	return nil
}

func getTopicPartitionLeaders(group *structs.Group) (map[int32]*sarama.Broker, error) { // Now returns map[int32]*sarama.Broker
	client := group.Client

	// Create a map of broker IDs to their corresponding *sarama.Broker
	brokerMap := make(map[int32]*sarama.Broker)
	for _, broker := range client.Brokers() {
		brokerMap[broker.ID()] = broker
	}

	// Fetch the metadata to identify partition leaders
	admin := group.Admin
	topicNames := make([]string, len(group.Topics))
	for i, topic := range group.Topics {
		topicNames[i] = topic.Name
	}

	topicMetadata, err := admin.DescribeTopics(topicNames)
	if err != nil {
		return nil, err
	}

	// Create a map of partition IDs to their leader brokers
	leaderMap := make(map[int32]*sarama.Broker)
	for _, topic := range topicMetadata {
		for _, partition := range topic.Partitions {
			leader := brokerMap[partition.Leader]
			if leader != nil {
				leaderMap[partition.ID] = leader
			}
		}
	}

	return leaderMap, nil
}
