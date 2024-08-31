package redis

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/sciclon2/kafka-lag-go/pkg/structs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestPersistLatestProducedOffsets(t *testing.T) {
	mockRedisClient := createMockRedisClient()
	mockPipeliner := new(MockPipeliner)

	// Set up the mock Redis client to return a mock Pipeliner
	mockRedisClient.On("Pipeline").Return(mockPipeliner)

	// Set up the mock ZRangeWithScores response
	mockZSliceCmd := redis.NewZSliceCmd(context.Background())
	mockZSliceCmd.SetVal([]redis.Z{
		{Score: 100, Member: "1609459200000"},
		{Score: 101, Member: "1609459260000"},
	})

	// Expect EvalSha and ZRangeWithScores calls
	mockPipeliner.On("EvalSha", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redis.NewCmd(context.Background()))
	mockPipeliner.On("ZRangeWithScores", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("int64"), mock.AnythingOfType("int64")).
		Return(mockZSliceCmd)
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
	groupStructCompleteChan := make(chan *structs.Group, 1)
	groupStructCompleteAndPersistedChan := make(chan *structs.Group, 1)

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
	groupStructCompleteChan <- group
	close(groupStructCompleteChan)

	// Run the function with a single worker
	redisManager.PersistLatestProducedOffsets(groupStructCompleteChan, groupStructCompleteAndPersistedChan, 1)

	// Retrieve the processed group
	processedGroup := <-groupStructCompleteAndPersistedChan

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
	mockRedisClient := createMockRedisClient()
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

	// Expect EvalSha and ZRangeWithScores calls
	mockPipeliner.On("EvalSha", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redis.NewCmd(context.Background()))
	mockPipeliner.On("ZRangeWithScores", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("int64"), mock.AnythingOfType("int64")).
		Return(mockZSliceCmd)
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
	groupStructCompleteChan := make(chan *structs.Group, 1)
	groupStructCompleteAndPersistedChan := make(chan *structs.Group, 1)

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
	groupStructCompleteChan <- group
	close(groupStructCompleteChan)

	// Run the function with a single worker
	redisManager.PersistLatestProducedOffsets(groupStructCompleteChan, groupStructCompleteAndPersistedChan, 1)

	// Retrieve the processed group
	processedGroup := <-groupStructCompleteAndPersistedChan

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
