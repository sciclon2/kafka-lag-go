package kafka

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/IBM/sarama"
	"github.com/sciclon2/kafka-lag-go/pkg/config"
	"github.com/sciclon2/kafka-lag-go/pkg/structs"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestGetConsumerGroupsInfo_LeaderInfoIsAssigned(t *testing.T) {
	// Create mock Kafka client and admin
	mockClient := new(MockKafkaClient)
	mockAdmin := new(MockKafkaAdmin)

	// Create two mock brokers using sarama.mocks and assign broker IDs
	mockBroker1 := sarama.NewMockBroker(t, 1) // Create a mock broker with ID 1
	mockBroker2 := sarama.NewMockBroker(t, 2) // Create a second mock broker with ID 2

	// Set up the Brokers() method to return a list of brokers with custom IDs
	customBroker1 := NewCustomBroker(mockBroker1.Addr(), 1) // Assign ID 1 to the custom broker 1
	customBroker1.Open(nil)                                 // Simulate connection
	setBrokerID(customBroker1.Broker, 1)

	customBroker2 := NewCustomBroker(mockBroker2.Addr(), 2) // Assign ID 2 to the custom broker 2
	customBroker2.Open(nil)                                 // Simulate connection
	setBrokerID(customBroker2.Broker, 2)

	mockClient.On("Brokers").Return([]*sarama.Broker{customBroker1.Broker, customBroker2.Broker})

	brokers := mockClient.Brokers()
	for _, broker := range brokers {
		logrus.Infof("Mocked Broker ID: %d, Addr: %s", broker.ID(), broker.Addr())
	}

	// Set up the RefreshMetadata() expectation
	mockClient.On("RefreshMetadata", mock.Anything).Return(nil)

	// Simulate DescribeTopics to return topic metadata with leader brokers for two groups
	topicName1 := "test-topic-1"
	topicName2 := "test-topic-2"
	partitionID1 := int32(0)
	partitionID2 := int32(0)

	// Return different metadata for different groups (topics)
	mockAdmin.On("DescribeTopics", []string{topicName1}).Return([]*sarama.TopicMetadata{
		{
			Name: topicName1,
			Partitions: []*sarama.PartitionMetadata{
				{
					ID:     partitionID1,
					Leader: customBroker1.ID(), // Use custom broker 1's ID
					Err:    sarama.ErrNoError,
				},
			},
		},
	}, nil)

	mockAdmin.On("DescribeTopics", []string{topicName2}).Return([]*sarama.TopicMetadata{
		{
			Name: topicName2,
			Partitions: []*sarama.PartitionMetadata{
				{
					ID:     partitionID2,
					Leader: customBroker2.ID(), // Use custom broker 2's ID
					Err:    sarama.ErrNoError,
				},
			},
		},
	}, nil)

	// Define two groups with different brokers
	group1 := &structs.Group{
		Name:   "test-group-1",
		Admin:  mockAdmin,
		Client: mockClient,
		Topics: []structs.Topic{
			{
				Name: topicName1,
				Partitions: []structs.Partition{
					{Number: partitionID1, LeaderBroker: nil}, // LeaderBroker will be assigned
				},
			},
		},
	}

	group2 := &structs.Group{
		Name:   "test-group-2",
		Admin:  mockAdmin,
		Client: mockClient,
		Topics: []structs.Topic{
			{
				Name: topicName2,
				Partitions: []structs.Partition{
					{Number: partitionID2, LeaderBroker: nil}, // LeaderBroker will be assigned
				},
			},
		},
	}

	// Create input and output channels
	groupsChan := make(chan *structs.Group, 2)
	groupsChan <- group1
	groupsChan <- group2
	close(groupsChan)

	// Call the function being tested
	outputChan := GetConsumerGroupsInfo(groupsChan, 1, 0, 1)

	// Retrieve the result from the output channel
	resultGroup1 := <-outputChan
	resultGroup2 := <-outputChan

	// Assertions for Group 1
	assert.NotNil(t, resultGroup1.Topics[0].Partitions[0].LeaderBroker, "LeaderBroker for group 1 should be assigned")
	assert.Equal(t, customBroker1.ID(), resultGroup1.Topics[0].Partitions[0].LeaderBroker.ID(), "LeaderBroker for group 1 should have the correct broker ID")
	assert.Equal(t, mockBroker1.Addr(), resultGroup1.Topics[0].Partitions[0].LeaderBroker.Addr(), "LeaderBroker for group 1 should have the correct broker address")

	// Assertions for Group 2
	assert.NotNil(t, resultGroup2.Topics[0].Partitions[0].LeaderBroker, "LeaderBroker for group 2 should be assigned")
	assert.Equal(t, customBroker2.ID(), resultGroup2.Topics[0].Partitions[0].LeaderBroker.ID(), "LeaderBroker for group 2 should have the correct broker ID")
	assert.Equal(t, mockBroker2.Addr(), resultGroup2.Topics[0].Partitions[0].LeaderBroker.Addr(), "LeaderBroker for group 2 should have the correct broker address")

	// Verify interactions with mocks
	mockClient.AssertExpectations(t)
	mockAdmin.AssertExpectations(t)

	// Close the mock brokers
	mockBroker1.Close()
	mockBroker2.Close()
}

func TestGetConsumerGroupsInfo_OneGroupFailsAnotherSucceeds(t *testing.T) {
	// Create mock Kafka client and admin
	mockClient := new(MockKafkaClient)
	mockAdmin := new(MockKafkaAdmin)

	// Set up mock brokers
	mockBroker1 := sarama.NewMockBroker(t, 1)
	mockBroker2 := sarama.NewMockBroker(t, 2)
	customBroker1 := NewCustomBroker(mockBroker1.Addr(), 1)
	customBroker1.Open(nil)
	setBrokerID(customBroker1.Broker, 1)

	customBroker2 := NewCustomBroker(mockBroker2.Addr(), 2)
	customBroker2.Open(nil)
	setBrokerID(customBroker2.Broker, 2)

	mockClient.On("Brokers").Return([]*sarama.Broker{customBroker1.Broker, customBroker2.Broker})

	// Set up the RefreshMetadata() expectation
	mockClient.On("RefreshMetadata", mock.Anything).Return(nil)

	// Simulate DescribeTopics returning an error for group 1
	topicName1 := "test-topic-1"
	mockAdmin.On("DescribeTopics", []string{topicName1}).Return([]*sarama.TopicMetadata{}, fmt.Errorf("failed to fetch metadata"))

	// Simulate DescribeTopics returning successful metadata for group 2
	topicName2 := "test-topic-2"
	mockAdmin.On("DescribeTopics", []string{topicName2}).Return([]*sarama.TopicMetadata{
		{
			Name: topicName2,
			Partitions: []*sarama.PartitionMetadata{
				{
					ID:     0,
					Leader: customBroker2.ID(),
					Err:    sarama.ErrNoError,
				},
			},
		},
	}, nil)

	// Define two groups: one that will fail and one that will succeed
	group1 := &structs.Group{
		Name:   "test-group-1",
		Admin:  mockAdmin,
		Client: mockClient,
		Topics: []structs.Topic{
			{
				Name: topicName1,
				Partitions: []structs.Partition{
					{Number: 0, LeaderBroker: nil}, // LeaderBroker will be assigned
				},
			},
		},
	}

	group2 := &structs.Group{
		Name:   "test-group-2",
		Admin:  mockAdmin,
		Client: mockClient,
		Topics: []structs.Topic{
			{
				Name: topicName2,
				Partitions: []structs.Partition{
					{Number: 0, LeaderBroker: nil}, // LeaderBroker will be assigned
				},
			},
		},
	}

	// Create input and output channels
	groupsChan := make(chan *structs.Group, 2)
	groupsChan <- group1
	groupsChan <- group2
	close(groupsChan)

	// Call the function being tested
	outputChan := GetConsumerGroupsInfo(groupsChan, 1, 0, 1)

	// Only one group should be returned since the other fails
	resultGroup := <-outputChan

	// Assert that only group 2 appears in the output (since group 1 fails)
	assert.Equal(t, "test-group-2", resultGroup.Name, "Expected group 2 to succeed and be returned")

	// Assert that the leader broker is correctly assigned for group 2
	assert.NotNil(t, resultGroup.Topics[0].Partitions[0].LeaderBroker, "LeaderBroker for group 2 should be assigned")
	assert.Equal(t, customBroker2.ID(), resultGroup.Topics[0].Partitions[0].LeaderBroker.ID(), "LeaderBroker for group 2 should have the correct broker ID")
	assert.Equal(t, mockBroker2.Addr(), resultGroup.Topics[0].Partitions[0].LeaderBroker.Addr(), "LeaderBroker for group 2 should have the correct broker address")

	// Verify interactions with mocks
	mockClient.AssertExpectations(t)
	mockAdmin.AssertExpectations(t)

	// Close the mock brokers
	mockBroker1.Close()
	mockBroker2.Close()
}

// TestAssembleGroups verifies that AssembleGroups assembles the correct groups based on the provided config and mocks.
func TestAssembleGroups(t *testing.T) {
	// Set up mock Kafka clients and admins
	mockAdmin1 := new(MockKafkaAdmin)
	mockAdmin2 := new(MockKafkaAdmin)

	// Mock ListConsumerGroups for each admin
	mockAdmin1.On("ListConsumerGroups").Return(map[string]string{
		"test-group-1": "description-1",
	}, nil)

	mockAdmin2.On("ListConsumerGroups").Return(map[string]string{
		"test-group-2": "description-2",
	}, nil)

	// Mock ListConsumerGroupOffsets for each group
	mockAdmin1.On("ListConsumerGroupOffsets", "test-group-1", mock.Anything).Return(&sarama.OffsetFetchResponse{
		Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{
			"test-topic-1": {
				0: {Offset: 100, Err: sarama.ErrNoError},
			},
		},
	}, nil)

	mockAdmin2.On("ListConsumerGroupOffsets", "test-group-2", mock.Anything).Return(&sarama.OffsetFetchResponse{
		Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{
			"test-topic-2": {
				0: {Offset: 200, Err: sarama.ErrNoError},
			},
		},
	}, nil)

	clientMap := map[string]structs.KafkaClient{
		"cluster-1": new(MockKafkaClient),
		"cluster-2": new(MockKafkaClient),
	}

	adminMap := map[string]structs.KafkaAdmin{
		"cluster-1": mockAdmin1,
		"cluster-2": mockAdmin2,
	}

	saramaConfigMap := map[string]*sarama.Config{
		"cluster-1": sarama.NewConfig(),
		"cluster-2": sarama.NewConfig(),
	}
	// Create a sample config to pass to the function
	cfg := &config.Config{
		KafkaClusters: []config.KafkaCluster{
			{
				Name:    "cluster-1",
				Brokers: []string{"broker1:9092"},
			},
			{
				Name:    "cluster-2",
				Brokers: []string{"broker2:9092"},
			},
		},
	}

	// Call AssembleGroups function with the mocks and config
	groupsChan := AssembleGroups(clientMap, adminMap, saramaConfigMap, cfg)

	// Collect groups from the channel
	var groups []*structs.Group
	for group := range groupsChan {
		groups = append(groups, group)
	}

	// Assertions to ensure that the groups are assembled correctly
	assert.Equal(t, 2, len(groups), "Expected to get 2 consumer groups")

	// Create a map for easier checking of group clusters as the channel does not guarantee order of groups
	groupsByCluster := map[string]*structs.Group{
		groups[0].ClusterName: groups[0],
		groups[1].ClusterName: groups[1],
	}

	// Check if group 1 belongs to cluster-1
	assert.Contains(t, groupsByCluster, "cluster-1", "Expected group 1 to belong to cluster-1")
	assert.Equal(t, "test-group-1", groupsByCluster["cluster-1"].Name, "Expected group name to be 'test-group-1'")

	// Check if group 2 belongs to cluster-2
	assert.Contains(t, groupsByCluster, "cluster-2", "Expected group 2 to belong to cluster-2")
	assert.Equal(t, "test-group-2", groupsByCluster["cluster-2"].Name, "Expected group name to be 'test-group-2'")

	// Verify that ListConsumerGroups and ListConsumerGroupOffsets were called
	mockAdmin1.AssertExpectations(t)
	mockAdmin2.AssertExpectations(t)
}

func TestAssembleGroups_NoConsumerGroups(t *testing.T) {
	// Set up mock Kafka clients and admins
	mockAdmin1 := new(MockKafkaAdmin)
	mockAdmin2 := new(MockKafkaAdmin)

	// Mock ListConsumerGroups for each admin to return no groups
	mockAdmin1.On("ListConsumerGroups").Return(map[string]string{}, nil)
	mockAdmin2.On("ListConsumerGroups").Return(map[string]string{}, nil)

	clientMap := map[string]structs.KafkaClient{
		"cluster-1": new(MockKafkaClient),
		"cluster-2": new(MockKafkaClient),
	}

	adminMap := map[string]structs.KafkaAdmin{
		"cluster-1": mockAdmin1,
		"cluster-2": mockAdmin2,
	}

	saramaConfigMap := map[string]*sarama.Config{
		"cluster-1": sarama.NewConfig(),
		"cluster-2": sarama.NewConfig(),
	}
	// Create a sample config to pass to the function
	cfg := &config.Config{
		KafkaClusters: []config.KafkaCluster{
			{
				Name:    "cluster-1",
				Brokers: []string{"broker1:9092"},
			},
			{
				Name:    "cluster-2",
				Brokers: []string{"broker2:9092"},
			},
		},
	}

	// Call AssembleGroups function with the mocks and config
	groupsChan := AssembleGroups(clientMap, adminMap, saramaConfigMap, cfg)

	// Collect groups from the channel
	var groups []*structs.Group
	for group := range groupsChan {
		groups = append(groups, group)
	}

	// Assertions to ensure no groups are returned
	assert.Equal(t, 0, len(groups), "Expected to get 0 consumer groups")
}

func TestAssembleGroups_WithBlacklist(t *testing.T) {
	// Set up mock Kafka clients and admins
	mockAdmin := new(MockKafkaAdmin)

	// Mock ListConsumerGroups to return some groups
	mockAdmin.On("ListConsumerGroups").Return(map[string]string{
		"test-group-1": "description-1",
		"test-group-2": "description-2",
		"test-group-3": "description-3",
	}, nil)

	// Mock ListConsumerGroupOffsets for each group
	mockAdmin.On("ListConsumerGroupOffsets", "test-group-1", mock.Anything).Return(&sarama.OffsetFetchResponse{
		Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{
			"test-topic-1": {
				0: {Offset: 100, Err: sarama.ErrNoError},
			},
		},
	}, nil)

	mockAdmin.On("ListConsumerGroupOffsets", "test-group-2", mock.Anything).Return(&sarama.OffsetFetchResponse{
		Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{
			"test-topic-2": {
				0: {Offset: 200, Err: sarama.ErrNoError},
			},
		},
	}, nil)

	mockAdmin.On("ListConsumerGroupOffsets", "test-group-3", mock.Anything).Return(&sarama.OffsetFetchResponse{
		Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{
			"test-topic-3": {
				0: {Offset: 300, Err: sarama.ErrNoError},
			},
		},
	}, nil)

	clientMap := map[string]structs.KafkaClient{
		"cluster-1": new(MockKafkaClient),
	}

	adminMap := map[string]structs.KafkaAdmin{
		"cluster-1": mockAdmin,
	}

	saramaConfigMap := map[string]*sarama.Config{
		"cluster-1": sarama.NewConfig(),
	}

	// Create a sample config to pass to the function with only a blacklist
	cfg := &config.Config{
		KafkaClusters: []config.KafkaCluster{
			{
				Name:    "cluster-1",
				Brokers: []string{"broker1:9092"},
				ConsumerGroups: config.ConsumerGroups{
					Blacklist: regexp.MustCompile("^test-group-2$"),
				},
			},
		},
	}

	// Call AssembleGroups function with the mocks and config
	groupsChan := AssembleGroups(clientMap, adminMap, saramaConfigMap, cfg)

	// Collect groups from the channel
	var groups []*structs.Group
	for group := range groupsChan {
		groups = append(groups, group)
	}

	// Assertions to ensure only the groups not matching the blacklist are included
	assert.Equal(t, 2, len(groups), "Expected to get 2 consumer groups due to blacklist filter")

	// Check that the blacklist excluded "test-group-2"
	groupsByName := map[string]*structs.Group{
		groups[0].Name: groups[0],
		groups[1].Name: groups[1],
	}

	assert.Contains(t, groupsByName, "test-group-1", "Expected 'test-group-1' to be in the results")
	assert.Contains(t, groupsByName, "test-group-3", "Expected 'test-group-3' to be in the results")
	assert.NotContains(t, groupsByName, "test-group-2", "Expected 'test-group-2' to be excluded by the blacklist")
}

func TestAssembleGroups_ErrorHandling(t *testing.T) {
	// Set up mock Kafka clients and admins
	mockAdmin := new(MockKafkaAdmin)

	// Mock ListConsumerGroups to return an error
	mockAdmin.On("ListConsumerGroups").Return(map[string]string{}, fmt.Errorf("mock error: failed to list consumer groups for cluster-2"))

	clientMap := map[string]structs.KafkaClient{
		"cluster-1": new(MockKafkaClient),
	}

	adminMap := map[string]structs.KafkaAdmin{
		"cluster-1": mockAdmin,
	}

	saramaConfigMap := map[string]*sarama.Config{
		"cluster-1": sarama.NewConfig(),
	}

	// Create a sample config to pass to the function
	cfg := &config.Config{
		KafkaClusters: []config.KafkaCluster{
			{
				Name:    "cluster-1",
				Brokers: []string{"broker1:9092"},
			},
		},
	}

	// Call AssembleGroups function with the mocks and config
	groupsChan := AssembleGroups(clientMap, adminMap, saramaConfigMap, cfg)

	// Collect groups from the channel
	var groups []*structs.Group
	for group := range groupsChan {
		groups = append(groups, group)
	}

	// Assertions to ensure no groups are returned due to the error in ListConsumerGroups
	assert.Equal(t, 0, len(groups), "Expected to get 0 consumer groups due to ListConsumerGroups error")

	// Verify that ListConsumerGroups was called
	mockAdmin.AssertExpectations(t)
}

func TestAssembleGroups_MixedConsumerGroupOffsetsErrorHandling(t *testing.T) {
	// Set up mock Kafka clients and admins
	mockAdmin := new(MockKafkaAdmin)

	// Mock ListConsumerGroups to return two groups
	mockAdmin.On("ListConsumerGroups").Return(map[string]string{
		"test-group-1": "description-1",
		"test-group-2": "description-2",
	}, nil)

	// Mock ListConsumerGroupOffsets to succeed for test-group-1
	mockAdmin.On("ListConsumerGroupOffsets", "test-group-1", mock.Anything).Return(&sarama.OffsetFetchResponse{
		Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{
			"test-topic-1": {
				0: {Offset: 100, Err: sarama.ErrNoError},
			},
		},
	}, nil)

	// Mock ListConsumerGroupOffsets to fail for test-group-2
	mockAdmin.On("ListConsumerGroupOffsets", "test-group-2", mock.Anything).Return(&sarama.OffsetFetchResponse{
		Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{},
	}, fmt.Errorf("mock error: failed to fetch offsets for test-group-2"))

	clientMap := map[string]structs.KafkaClient{
		"cluster-1": new(MockKafkaClient),
	}

	adminMap := map[string]structs.KafkaAdmin{
		"cluster-1": mockAdmin,
	}

	saramaConfigMap := map[string]*sarama.Config{
		"cluster-1": sarama.NewConfig(),
	}

	// Create a sample config to pass to the function
	cfg := &config.Config{
		KafkaClusters: []config.KafkaCluster{
			{
				Name:    "cluster-1",
				Brokers: []string{"broker1:9092"},
			},
		},
	}

	// Call AssembleGroups function with the mocks and config
	groupsChan := AssembleGroups(clientMap, adminMap, saramaConfigMap, cfg)

	// Collect groups from the channel
	var groups []*structs.Group
	for group := range groupsChan {
		groups = append(groups, group)
	}

	// Assertions to ensure that only one group is successfully processed
	assert.Equal(t, 1, len(groups), "Expected to get 1 successfully processed consumer group due to ListConsumerGroupOffsets error for the second group")

	// Check if group 1 was processed successfully
	assert.Equal(t, "test-group-1", groups[0].Name, "Expected group name to be 'test-group-1'")

	// Verify that ListConsumerGroups and ListConsumerGroupOffsets were called
	mockAdmin.AssertExpectations(t)
}
func TestAssembleGroups_MixedListConsumerGroupsErrorHandling(t *testing.T) {
	// Set up mock Kafka clients and admins
	mockAdmin1 := new(MockKafkaAdmin)
	mockAdmin2 := new(MockKafkaAdmin)

	// Mock ListConsumerGroups to succeed for cluster-1
	mockAdmin1.On("ListConsumerGroups").Return(map[string]string{
		"test-group-1": "description-1",
	}, nil)

	// Mock ListConsumerGroups to fail for cluster-2
	mockAdmin2.On("ListConsumerGroups").Return(map[string]string{}, fmt.Errorf("mock error: failed to list consumer groups for cluster-2"))
	// Mock ListConsumerGroupOffsets for test-group-1
	mockAdmin1.On("ListConsumerGroupOffsets", "test-group-1", mock.Anything).Return(&sarama.OffsetFetchResponse{
		Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{
			"test-topic-1": {
				0: {Offset: 100, Err: sarama.ErrNoError},
			},
		},
	}, nil)

	clientMap := map[string]structs.KafkaClient{
		"cluster-1": new(MockKafkaClient),
		"cluster-2": new(MockKafkaClient),
	}

	// Make sure both mockAdmin1 and mockAdmin2 are passed into the adminMap
	adminMap := map[string]structs.KafkaAdmin{
		"cluster-1": mockAdmin1,
		"cluster-2": mockAdmin2,
	}

	saramaConfigMap := map[string]*sarama.Config{
		"cluster-1": sarama.NewConfig(),
		"cluster-2": sarama.NewConfig(),
	}

	// Create a sample config to pass to the function
	cfg := &config.Config{
		KafkaClusters: []config.KafkaCluster{
			{
				Name:    "cluster-1",
				Brokers: []string{"broker1:9092"},
			},
			{
				Name:    "cluster-2",
				Brokers: []string{"broker2:9092"},
			},
		},
	}

	// Call AssembleGroups function with the mocks and config
	groupsChan := AssembleGroups(clientMap, adminMap, saramaConfigMap, cfg)

	// Collect groups from the channel
	var groups []*structs.Group
	for group := range groupsChan {
		groups = append(groups, group)
	}

	// Assertions to ensure that only one group is successfully processed from cluster-1
	assert.Equal(t, 1, len(groups), "Expected to get 1 successfully processed consumer group due to ListConsumerGroups error for cluster-2")

	// Check if group 1 from cluster-1 was processed successfully
	assert.Equal(t, "test-group-1", groups[0].Name, "Expected group name to be 'test-group-1'")

	// Verify that ListConsumerGroups and ListConsumerGroupOffsets were called correctly
	mockAdmin1.AssertExpectations(t)
	mockAdmin2.AssertExpectations(t)
}
