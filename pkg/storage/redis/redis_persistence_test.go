package redis

import (
	"context"
	"sort"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/sciclon2/kafka-lag-go/pkg/structs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestPersistLatestProducedOffsets(t *testing.T) {
	mockRedisClient := CreateMockRedisClient()
	mockPipeliner := new(MockPipeliner)

	// Set up the mock Redis client to return a mock Pipeliner
	mockRedisClient.On("Pipeline").Return(mockPipeliner)

	// Set up the mock ZRangeWithScores response
	mockZSliceCmd := redis.NewZSliceCmd(context.Background())
	mockZSliceCmd.SetVal([]redis.Z{
		{Score: 100, Member: "1609459200000"},
		{Score: 101, Member: "1609459260000"},
	})
	mockPipeliner.On("ZRangeWithScores", mock.Anything, ":test-topic:0", int64(0), int64(-1)).Return(mockZSliceCmd)

	// Expect EvalSha and ZRangeWithScores calls
	mockPipeliner.On("EvalSha", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redis.NewCmd(context.Background()))

	mockPipeliner.On("Exec", mock.Anything).
		Return([]redis.Cmder{mockZSliceCmd}, nil)

	// Create an instance of RedisManager using the mock client
	redisManager := &RedisManager{
		client:     mockRedisClient,
		LuaSHA:     "mockedSHA",
		TTLSeconds: 3600,
		ctx:        context.Background(),
	}

	// Channels for test
	groupsWithLeaderInfoAndLeaderOffsetsChan := make(chan *structs.Group, 1)

	// Create a dummy Group object
	group := &structs.Group{
		Name: "test-group",
		Topics: []structs.Topic{
			{
				Name: "test-topic",
				Partitions: []structs.Partition{
					{Number: 0, LatestProducedOffset: 100, LatestProducedOffsetAt: 1609459200000},
				},
			},
		},
	}

	// Send the dummy group into the channel
	groupsWithLeaderInfoAndLeaderOffsetsChan <- group
	close(groupsWithLeaderInfoAndLeaderOffsetsChan)

	// Run the function with a single worker
	groupsComplete := redisManager.PersistLatestProducedOffsets(groupsWithLeaderInfoAndLeaderOffsetsChan, 1)

	// Read the processed group from the channel
	processedGroup := <-groupsComplete

	// Verify that the group was processed
	assert.NotNil(t, processedGroup)
	assert.Equal(t, "test-group", processedGroup.Name)
	assert.Equal(t, 1, len(processedGroup.Topics))

	// Verify that the ProducedOffsetsHistory is correctly populated
	partition := processedGroup.Topics[0].Partitions[0]
	assert.Len(t, partition.ProducedOffsetsHistory, 2)
	assert.Equal(t, int64(100), int64(partition.ProducedOffsetsHistory[0].Score)) // Convert to int64 for comparison
	assert.Equal(t, "1609459200000", partition.ProducedOffsetsHistory[0].Member)
	assert.Equal(t, int64(101), int64(partition.ProducedOffsetsHistory[1].Score)) // Convert to int64 for comparison
	assert.Equal(t, "1609459260000", partition.ProducedOffsetsHistory[1].Member)

	// Validate that the Redis commands executed as expected
	mockPipeliner.AssertExpectations(t)
	mockRedisClient.AssertExpectations(t)
}

func TestPersistLatestProducedOffsetsWithCompression(t *testing.T) {
	mockRedisClient := CreateMockRedisClient()
	mockPipeliner := new(MockPipeliner)

	// Set up the mock Redis client to return a mock Pipeliner
	mockRedisClient.On("Pipeline").Return(mockPipeliner)

	// Set up the mock ZRangeWithScores response with repeated entries
	mockZSliceCmd := redis.NewZSliceCmd(context.Background())
	mockZSliceCmd.SetVal([]redis.Z{
		{Score: 100, Member: "1609459200000"},
		{Score: 100, Member: "1609459205000"},
		{Score: 101, Member: "1609459260000"},
		{Score: 101, Member: "1609459265000"},
	})
	mockPipeliner.On("ZRangeWithScores", mock.Anything, ":test-topic:0", int64(0), int64(-1)).Return(mockZSliceCmd)

	// Mock EvalSha and Exec behavior
	mockPipeliner.On("EvalSha", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redis.NewCmd(context.Background()))
	mockPipeliner.On("Exec", mock.Anything).
		Return([]redis.Cmder{redis.NewCmd(context.Background())}, nil)

	// Create an instance of RedisManager using the mock client
	redisManager := &RedisManager{
		client:     mockRedisClient,
		LuaSHA:     "mockedSHA",
		TTLSeconds: 3600,
		ctx:        context.Background(),
	}

	// Channels for test
	groupsWithLeaderInfoAndLeaderOffsetsChan := make(chan *structs.Group, 1)

	// Create a dummy Group object
	group := &structs.Group{
		Name: "test-group",
		Topics: []structs.Topic{
			{
				Name: "test-topic",
				Partitions: []structs.Partition{
					{Number: 0, LatestProducedOffset: 100, LatestProducedOffsetAt: 1609459200000},
				},
			},
		},
	}

	// Send the dummy group into the channel
	groupsWithLeaderInfoAndLeaderOffsetsChan <- group
	close(groupsWithLeaderInfoAndLeaderOffsetsChan)

	// Run the function with a single worker
	groupsComplete := redisManager.PersistLatestProducedOffsets(groupsWithLeaderInfoAndLeaderOffsetsChan, 1)

	// Retrieve the processed group
	processedGroup := <-groupsComplete

	// Verify that the group was processed
	assert.NotNil(t, processedGroup)
	assert.Equal(t, "test-group", processedGroup.Name)
	assert.Equal(t, 1, len(processedGroup.Topics))

	// Verify that the ProducedOffsetsHistory is correctly populated and compressed
	partition := processedGroup.Topics[0].Partitions[0]
	assert.Len(t, partition.ProducedOffsetsHistory, 2) // Should be compressed to 2 unique entries
	assert.Equal(t, int64(100), int64(partition.ProducedOffsetsHistory[0].Score))
	assert.Equal(t, "1609459205000", partition.ProducedOffsetsHistory[0].Member)
	assert.Equal(t, int64(101), int64(partition.ProducedOffsetsHistory[1].Score))
	assert.Equal(t, "1609459265000", partition.ProducedOffsetsHistory[1].Member)

	// Validate that the Redis commands executed as expected
	mockPipeliner.AssertExpectations(t)
	mockRedisClient.AssertExpectations(t)
}

func TestPersistLatestProducedOffsetsMultipleClusters(t *testing.T) {
	mockRedisClient := CreateMockRedisClient()
	mockPipeliner := new(MockPipeliner)

	// Set up the mock Redis client to return a mock Pipeliner
	mockRedisClient.On("Pipeline").Return(mockPipeliner)

	// Set up the mock ZRangeWithScores response for each cluster
	mockZSliceCmdCluster1 := redis.NewZSliceCmd(context.Background())
	mockZSliceCmdCluster1.SetVal([]redis.Z{
		{Score: 100, Member: "1609459200000"},
		{Score: 101, Member: "1609459260000"},
	})
	mockZSliceCmdCluster2 := redis.NewZSliceCmd(context.Background())
	mockZSliceCmdCluster2.SetVal([]redis.Z{
		{Score: 200, Member: "1609459200000"},
		{Score: 201, Member: "1609459260000"},
	})

	// Expect EvalSha and ZRangeWithScores calls for both clusters
	mockPipeliner.On("EvalSha", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redis.NewCmd(context.Background()))
	mockPipeliner.On("ZRangeWithScores", mock.Anything, "test-cluster-1:test-topic-1:0", int64(0), int64(-1)).
		Return(mockZSliceCmdCluster1)
	mockPipeliner.On("ZRangeWithScores", mock.Anything, "test-cluster-2:test-topic-2:0", int64(0), int64(-1)).
		Return(mockZSliceCmdCluster2)
	mockPipeliner.On("Exec", mock.Anything).
		Return([]redis.Cmder{mockZSliceCmdCluster1, mockZSliceCmdCluster2}, nil)

	// Create an instance of RedisManager using the mock client
	redisManager := &RedisManager{
		client:     mockRedisClient,
		LuaSHA:     "mockedSHA",
		TTLSeconds: 3600,
		ctx:        context.Background(),
	}

	// Channels for test
	groupsWithLeaderInfoAndLeaderOffsetsChan := make(chan *structs.Group, 2)

	// Create dummy Group objects for two clusters
	groupCluster1 := &structs.Group{
		Name:        "test-group-1",
		ClusterName: "test-cluster-1",
		Topics: []structs.Topic{
			{
				Name: "test-topic-1",
				Partitions: []structs.Partition{
					{Number: 0, LatestProducedOffset: 100, LatestProducedOffsetAt: 1609459200000},
				},
			},
		},
	}
	groupCluster2 := &structs.Group{
		Name:        "test-group-2",
		ClusterName: "test-cluster-2",
		Topics: []structs.Topic{
			{
				Name: "test-topic-2",
				Partitions: []structs.Partition{
					{Number: 0, LatestProducedOffset: 200, LatestProducedOffsetAt: 1609459200000},
				},
			},
		},
	}

	// Send the dummy groups into the channel
	groupsWithLeaderInfoAndLeaderOffsetsChan <- groupCluster1
	groupsWithLeaderInfoAndLeaderOffsetsChan <- groupCluster2
	close(groupsWithLeaderInfoAndLeaderOffsetsChan)

	// Run the function with multiple workers
	groupsComplete := redisManager.PersistLatestProducedOffsets(groupsWithLeaderInfoAndLeaderOffsetsChan, 2)

	// Collect results from the output channel
	var processedGroups []*structs.Group
	for group := range groupsComplete {
		processedGroups = append(processedGroups, group)
	}

	// Sort the processedGroups by Name before making assertions as  channel output will not be ordered
	sort.Slice(processedGroups, func(i, j int) bool {
		return processedGroups[i].Name < processedGroups[j].Name
	})
	// Verify that both groups were processed
	assert.Len(t, processedGroups, 2)

	// Check group 1
	assert.Equal(t, "test-group-1", processedGroups[0].Name)
	partition1 := processedGroups[0].Topics[0].Partitions[0]
	assert.Len(t, partition1.ProducedOffsetsHistory, 2)
	assert.Equal(t, int64(100), int64(partition1.ProducedOffsetsHistory[0].Score))
	assert.Equal(t, "1609459200000", partition1.ProducedOffsetsHistory[0].Member)
	assert.Equal(t, int64(101), int64(partition1.ProducedOffsetsHistory[1].Score))
	assert.Equal(t, "1609459260000", partition1.ProducedOffsetsHistory[1].Member)

	// Check group 2
	assert.Equal(t, "test-group-2", processedGroups[1].Name)
	partition2 := processedGroups[1].Topics[0].Partitions[0]
	assert.Len(t, partition2.ProducedOffsetsHistory, 2)
	assert.Equal(t, int64(200), int64(partition2.ProducedOffsetsHistory[0].Score))
	assert.Equal(t, "1609459200000", partition2.ProducedOffsetsHistory[0].Member)
	assert.Equal(t, int64(201), int64(partition2.ProducedOffsetsHistory[1].Score))
	assert.Equal(t, "1609459260000", partition2.ProducedOffsetsHistory[1].Member)

	// Validate that the Redis commands executed as expected
	mockPipeliner.AssertExpectations(t)
	mockRedisClient.AssertExpectations(t)
}
