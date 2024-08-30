package kafka

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/sciclon2/kafka-lag-go/pkg/structs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestGetLatestProducedOffsets_Success(t *testing.T) {
	mockBroker := new(MockSaramaBroker)

	config := sarama.NewConfig()

	// Mock connection methods
	mockBroker.On("Connected").Return(true, nil)
	mockBroker.On("GetAvailableOffsets", mock.Anything).Return(&sarama.OffsetResponse{}, nil)
	mockBroker.On("Addr").Return("mockBroker:9092")

	// Set up the group and partition details
	partitionNumber := int32(0)
	commitedOffset := int64(100)
	latestProducedOffset := int64(105)
	latestProducedOffsetAt := int64(1724360000000)

	partitions := []structs.Partition{
		{
			Number:                 partitionNumber,
			CommitedOffset:         commitedOffset,
			LatestProducedOffset:   latestProducedOffset,
			LatestProducedOffsetAt: latestProducedOffsetAt,
			LeaderBroker:           mockBroker, // Now using the mock broker
		},
	}

	topics := []structs.Topic{
		{
			Name:       "test-topic",
			Partitions: partitions,
		},
	}

	group := &structs.Group{
		Name:   "test-group",
		Topics: topics,
	}

	// Channels for input and output
	groupStructPartialChan := make(chan *structs.Group, 1)
	groupStructCompleteChan := make(chan *structs.Group, 1)

	groupStructPartialChan <- group
	close(groupStructPartialChan)

	GetLatestProducedOffsets(nil, groupStructPartialChan, groupStructCompleteChan, 1, config)

	processedGroup := <-groupStructCompleteChan

	// Assert that the LatestProducedOffset and LatestProducedOffsetAt are set correctly
	partition := processedGroup.Topics[0].Partitions[0]
	assert.Equal(t, latestProducedOffset, partition.LatestProducedOffset)
	assert.Equal(t, latestProducedOffsetAt, partition.LatestProducedOffsetAt)

	// Ensure all expectations on the mock were met
	mockBroker.AssertExpectations(t)
}
func TestGetLatestProducedOffsets_MultipleBrokers(t *testing.T) {
	mockBroker1 := new(MockSaramaBroker)
	mockBroker2 := new(MockSaramaBroker)

	config := sarama.NewConfig()

	// Mock connection methods for both brokers
	mockBroker1.On("Connected").Return(true, nil)
	mockBroker1.On("GetAvailableOffsets", mock.Anything).Return(&sarama.OffsetResponse{}, nil)
	mockBroker1.On("Addr").Return("mockBroker1:9092")

	mockBroker2.On("Connected").Return(true, nil)
	mockBroker2.On("GetAvailableOffsets", mock.Anything).Return(&sarama.OffsetResponse{}, nil)
	mockBroker2.On("Addr").Return("mockBroker2:9092")

	// Set up the group and partition details for both brokers
	partitions1 := []structs.Partition{
		{Number: 0, CommitedOffset: 100, LeaderBroker: mockBroker1},
		{Number: 1, CommitedOffset: 200, LeaderBroker: mockBroker1},
	}
	partitions2 := []structs.Partition{
		{Number: 0, CommitedOffset: 300, LeaderBroker: mockBroker2},
		{Number: 1, CommitedOffset: 400, LeaderBroker: mockBroker2},
	}

	topics := []structs.Topic{
		{Name: "test-topic1", Partitions: partitions1},
		{Name: "test-topic2", Partitions: partitions2},
	}

	group := &structs.Group{
		Name:   "test-group",
		Topics: topics,
	}

	groupStructPartialChan := make(chan *structs.Group, 1)
	groupStructCompleteChan := make(chan *structs.Group, 1)

	groupStructPartialChan <- group
	close(groupStructPartialChan)

	GetLatestProducedOffsets(nil, groupStructPartialChan, groupStructCompleteChan, 1, config)
	processedGroup := <-groupStructCompleteChan

	// Assert that the offsets were processed correctly
	for _, topic := range processedGroup.Topics {
		for _, partition := range topic.Partitions {
			assert.NotEqual(t, int64(-1), partition.LatestProducedOffset)
			assert.NotEqual(t, int64(-1), partition.LatestProducedOffsetAt)
		}
	}

	mockBroker1.AssertExpectations(t)
	mockBroker2.AssertExpectations(t)
}
func TestGetLatestProducedOffsets_BrokerFailure(t *testing.T) {
	mockBroker := new(MockSaramaBroker)

	config := sarama.NewConfig()

	// Mock connection method to succeed
	mockBroker.On("Connected").Return(true, nil)
	// Mock GetAvailableOffsets to simulate a broker failure by returning a valid *sarama.OffsetResponse with an error
	mockResponse := &sarama.OffsetResponse{
		Blocks: map[string]map[int32]*sarama.OffsetResponseBlock{
			"test-topic": {
				0: {Err: sarama.ErrBrokerNotAvailable}, // Simulate a broker failure
			},
		},
	}
	mockBroker.On("GetAvailableOffsets", mock.Anything).Return(mockResponse, nil)
	mockBroker.On("Addr").Return("mockBroker:9092")

	// Set up the group and partition details
	partitions := []structs.Partition{
		{Number: 0, CommitedOffset: 100, LeaderBroker: mockBroker, LatestProducedOffset: -1},
	}

	topics := []structs.Topic{
		{Name: "test-topic", Partitions: partitions},
	}

	group := &structs.Group{
		Name:   "test-group",
		Topics: topics,
	}

	groupStructPartialChan := make(chan *structs.Group, 1)
	groupStructCompleteChan := make(chan *structs.Group, 1)

	groupStructPartialChan <- group
	close(groupStructPartialChan)

	GetLatestProducedOffsets(nil, groupStructPartialChan, groupStructCompleteChan, 1, config)
	processedGroup := <-groupStructCompleteChan

	// Since the broker failed, LatestProducedOffset should remain as the initial value
	partition := processedGroup.Topics[0].Partitions[0]
	assert.Equal(t, int64(-1), partition.LatestProducedOffset) // Or whatever default value you use when the offset is not fetched

	mockBroker.AssertExpectations(t)
}
func TestGetLatestProducedOffsets_NoOffsetsAvailable(t *testing.T) {
	mockBroker := new(MockSaramaBroker)

	config := sarama.NewConfig()

	// Mock connection method to succeed
	mockBroker.On("Connected").Return(true, nil)
	// Mock GetAvailableOffsets to return an empty OffsetResponse
	mockBroker.On("GetAvailableOffsets", mock.Anything).Return(&sarama.OffsetResponse{}, nil)
	mockBroker.On("Addr").Return("mockBroker:9092")

	// Set up the group and partition details
	partitions := []structs.Partition{
		{Number: 0, CommitedOffset: 100, LeaderBroker: mockBroker, LatestProducedOffset: -1, LatestProducedOffsetAt: -1},
	}

	topics := []structs.Topic{
		{Name: "test-topic", Partitions: partitions},
	}

	group := &structs.Group{
		Name:   "test-group",
		Topics: topics,
	}

	groupStructPartialChan := make(chan *structs.Group, 1)
	groupStructCompleteChan := make(chan *structs.Group, 1)

	groupStructPartialChan <- group
	close(groupStructPartialChan)

	GetLatestProducedOffsets(nil, groupStructPartialChan, groupStructCompleteChan, 1, config)

	processedGroup := <-groupStructCompleteChan

	// Assert that the LatestProducedOffset and LatestProducedOffsetAt remain unchanged
	partition := processedGroup.Topics[0].Partitions[0]
	assert.Equal(t, int64(-1), partition.LatestProducedOffset)
	assert.Equal(t, int64(-1), partition.LatestProducedOffsetAt)

	mockBroker.AssertExpectations(t)
}
func TestGetLatestProducedOffsets_MixedSuccessAndFailure(t *testing.T) {
	mockBroker := new(MockSaramaBroker)

	config := sarama.NewConfig()

	// Mock connection method to succeed
	mockBroker.On("Connected").Return(true, nil)
	// Mock GetAvailableOffsets to return a partial success
	mockBroker.On("GetAvailableOffsets", mock.Anything).Return(&sarama.OffsetResponse{
		Blocks: map[string]map[int32]*sarama.OffsetResponseBlock{
			"test-topic": {
				0: {Offsets: []int64{105}, Err: sarama.ErrNoError},
				1: {Offsets: []int64{0}, Err: sarama.ErrOffsetOutOfRange},
			},
		},
	}, nil)
	mockBroker.On("Addr").Return("mockBroker:9092")

	// Set up the group and partition details
	partitions := []structs.Partition{
		{Number: 0, CommitedOffset: 100, LeaderBroker: mockBroker, LatestProducedOffset: -1, LatestProducedOffsetAt: -1},
		{Number: 1, CommitedOffset: 200, LeaderBroker: mockBroker, LatestProducedOffset: -1, LatestProducedOffsetAt: -1},
	}

	topics := []structs.Topic{
		{Name: "test-topic", Partitions: partitions},
	}

	group := &structs.Group{
		Name:   "test-group",
		Topics: topics,
	}

	groupStructPartialChan := make(chan *structs.Group, 1)
	groupStructCompleteChan := make(chan *structs.Group, 1)

	groupStructPartialChan <- group
	close(groupStructPartialChan)

	GetLatestProducedOffsets(nil, groupStructPartialChan, groupStructCompleteChan, 1, config)

	processedGroup := <-groupStructCompleteChan

	// Assert that the first partition's offset is updated, but the second remains unchanged
	assert.Equal(t, int64(105), processedGroup.Topics[0].Partitions[0].LatestProducedOffset)
	assert.Equal(t, int64(-1), processedGroup.Topics[0].Partitions[1].LatestProducedOffset)

	mockBroker.AssertExpectations(t)
}
