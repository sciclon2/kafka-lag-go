package metrics

import (
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/sciclon2/kafka-lag-go/pkg/structs"
	"github.com/stretchr/testify/assert"
)

// TestGenerateMetrics verifies that the GenerateMetrics function correctly processes groups and calculates metrics.
func TestGenerateMetrics(t *testing.T) {
	// Create a LagProcessor instance
	lp := NewLagProcessor()

	// Set up a test group with topics and partitions
	partitionNumber := int32(0)
	commitedOffset := int64(100)
	latestProducedOffset := int64(110)
	latestProducedOffsetAt := int64(1724360000000)
	producedOffsetsHistory := []redis.Z{
		{Score: 99, Member: "1724350000000"},
		{Score: 105, Member: "1724360000000"},
	}

	partitions := []structs.Partition{
		{
			Number:                 partitionNumber,
			CommitedOffset:         commitedOffset,
			LatestProducedOffset:   latestProducedOffset,
			LatestProducedOffsetAt: latestProducedOffsetAt,
			ProducedOffsetsHistory: producedOffsetsHistory,
			LagInOffsets:           -1, // Will be calculated by processGroup
			LagInSeconds:           -1, // Will be calculated by processGroup
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

	// Channels for input
	groupStructCompleteAndPersistedChan := make(chan *structs.Group, 1)

	// Send the group into the input channel
	groupStructCompleteAndPersistedChan <- group
	close(groupStructCompleteAndPersistedChan)

	// Call the GenerateMetrics method with 2 workers
	metricsToExportChan := lp.GenerateMetrics(groupStructCompleteAndPersistedChan, 2)

	// Read the result from the output channel
	processedGroup := <-metricsToExportChan

	// Expected results
	expectedLagInOffsets := latestProducedOffset - commitedOffset
	expectedLagInSeconds := int64(8333)

	// Assert the expected lag metrics
	partition := processedGroup.Topics[0].Partitions[0]
	assert.Equal(t, expectedLagInOffsets, partition.LagInOffsets, "LagInOffsets should be calculated correctly")
	assert.Equal(t, expectedLagInSeconds, partition.LagInSeconds, "LagInSeconds should be calculated correctly")

	// Assert MaxLagInOffsets and MaxLagInSeconds at the group level
	assert.Equal(t, expectedLagInOffsets, processedGroup.MaxLagInOffsets, "MaxLagInOffsets should be calculated correctly at the group level")
	assert.Equal(t, expectedLagInSeconds, processedGroup.MaxLagInSeconds, "MaxLagInSeconds should be calculated correctly at the group level")
}

func TestLagProcessor_InsufficientProducedOffsetsHistory(t *testing.T) {
	lp := NewLagProcessor()

	partitions := []structs.Partition{
		{
			Number:                 0,
			CommitedOffset:         100,
			LatestProducedOffset:   110,
			LatestProducedOffsetAt: 1724360000000,
			ProducedOffsetsHistory: []redis.Z{
				{Score: 100, Member: "1724350000000"},
			},
			LagInOffsets: -1,
			LagInSeconds: -1,
		},
	}

	group := &structs.Group{
		Name: "test-group",
		Topics: []structs.Topic{
			{
				Name:       "test-topic",
				Partitions: partitions,
			},
		},
	}

	// Call the processGroup function
	lp.processGroup(group)

	// The expected result is that LagInSeconds should be -1 due to insufficient history
	assert.Equal(t, int64(-1), group.Topics[0].Partitions[0].LagInSeconds)
}

func TestLagProcessor_NoLagCondition(t *testing.T) {
	lp := NewLagProcessor()

	// Set up a partition where committed offset equals the latest produced offset
	partitions := []structs.Partition{
		{
			Number:                 0,
			CommitedOffset:         110,
			LatestProducedOffset:   110,
			LatestProducedOffsetAt: 1724360000000,
			ProducedOffsetsHistory: []redis.Z{
				{Score: 100, Member: "1724350000000"},
				{Score: 110, Member: "1724360000000"},
			},
			LagInOffsets: -1,
			LagInSeconds: -1,
		},
	}

	group := &structs.Group{
		Name: "test-group",
		Topics: []structs.Topic{
			{
				Name:       "test-topic",
				Partitions: partitions,
			},
		},
	}

	lp.processGroup(group)

	// Expect both lags to be zero
	assert.Equal(t, int64(0), group.Topics[0].Partitions[0].LagInOffsets)
	assert.Equal(t, int64(0), group.Topics[0].Partitions[0].LagInSeconds)
}

func TestLagProcessor_NegativeOffsetCondition(t *testing.T) {
	lp := NewLagProcessor()

	// Set up a partition where the committed offset is greater than the latest produced offset
	partitions := []structs.Partition{
		{
			Number:                 0,
			CommitedOffset:         120,
			LatestProducedOffset:   110,
			LatestProducedOffsetAt: 1724360000000,
			ProducedOffsetsHistory: []redis.Z{
				{Score: 100, Member: "1724350000000"},
				{Score: 110, Member: "1724360000000"},
			},
			LagInOffsets: -1,
			LagInSeconds: -1,
		},
	}

	group := &structs.Group{
		Name: "test-group",
		Topics: []structs.Topic{
			{
				Name:       "test-topic",
				Partitions: partitions,
			},
		},
	}

	lp.processGroup(group)

	// Expect LagInOffsets to be 0 because it cannot be negative
	assert.Equal(t, int64(0), group.Topics[0].Partitions[0].LagInOffsets)
	assert.GreaterOrEqual(t, group.Topics[0].Partitions[0].LagInSeconds, int64(0))
}
func TestLagProcessor_EmptyProducedOffsetsHistory(t *testing.T) {
	lp := NewLagProcessor()

	// Set up a partition with an empty produced offsets history
	partitions := []structs.Partition{
		{
			Number:                 0,
			CommitedOffset:         100,
			LatestProducedOffset:   110,
			LatestProducedOffsetAt: 1724360000000,
			ProducedOffsetsHistory: []redis.Z{}, // No history available
			LagInOffsets:           -1,
			LagInSeconds:           -1,
		},
	}

	group := &structs.Group{
		Name: "test-group",
		Topics: []structs.Topic{
			{
				Name:       "test-topic",
				Partitions: partitions,
			},
		},
	}

	lp.processGroup(group)

	// Expect LagInSeconds to remain at -1 due to lack of history
	assert.Equal(t, int64(-1), group.Topics[0].Partitions[0].LagInSeconds)
}

func TestLagProcessor_ExtrapolationInProducedOffsetsHistory(t *testing.T) {
	lp := NewLagProcessor()

	// Set up a partition where the committed offset is beyond the last known offset in the produced offsets history
	partitionNumber := int32(0)
	commitedOffset := int64(110) // Beyond the last known offset in the history
	latestProducedOffset := int64(115)
	latestProducedOffsetAt := int64(1724365000000)
	producedOffsetsHistory := []redis.Z{
		{Score: 100, Member: "1724350000000"},
		{Score: 105, Member: "1724360000000"},
	}

	partitions := []structs.Partition{
		{
			Number:                 partitionNumber,
			CommitedOffset:         commitedOffset,
			LatestProducedOffset:   latestProducedOffset,
			LatestProducedOffsetAt: latestProducedOffsetAt,
			ProducedOffsetsHistory: producedOffsetsHistory,
			LagInOffsets:           -1,
			LagInSeconds:           -1,
		},
	}

	group := &structs.Group{
		Name: "test-group",
		Topics: []structs.Topic{
			{
				Name:       "test-topic",
				Partitions: partitions,
			},
		},
	}

	lp.processGroup(group)

	// Expected results
	expectedLagInOffsets := latestProducedOffset - commitedOffset
	expectedLagInSeconds := (latestProducedOffsetAt - 1724360000000) / 1000 // Extrapolated based on the rate between known points

	// Assert the expected lag metrics
	partition := group.Topics[0].Partitions[0]
	assert.Equal(t, expectedLagInOffsets, partition.LagInOffsets, "LagInOffsets should be calculated correctly")
	assert.Equal(t, expectedLagInSeconds, partition.LagInSeconds, "LagInSeconds should be extrapolated correctly")
}

func TestLagProcessor_InterpolationInProducedOffsetsHistory(t *testing.T) {
	lp := NewLagProcessor()

	// Set up a partition where the committed offset falls between two known offsets in the produced offsets history
	partitionNumber := int32(0)
	commitedOffset := int64(103) // Falls between 100 and 105 in the history
	latestProducedOffset := int64(110)
	latestProducedOffsetAt := int64(1724365000000)
	producedOffsetsHistory := []redis.Z{
		{Score: 100, Member: "1724350000000"},
		{Score: 105, Member: "1724360000000"},
	}

	partitions := []structs.Partition{
		{
			Number:                 partitionNumber,
			CommitedOffset:         commitedOffset,
			LatestProducedOffset:   latestProducedOffset,
			LatestProducedOffsetAt: latestProducedOffsetAt,
			ProducedOffsetsHistory: producedOffsetsHistory,
			LagInOffsets:           -1,
			LagInSeconds:           -1,
		},
	}

	group := &structs.Group{
		Name: "test-group",
		Topics: []structs.Topic{
			{
				Name:       "test-topic",
				Partitions: partitions,
			},
		},
	}

	lp.processGroup(group)

	// Expected results
	expectedLagInOffsets := latestProducedOffset - commitedOffset

	// Interpolation: Since 103 is between 100 and 105, we calculate the timestamp based on the known points
	lowerOffset := int64(100)
	lowerTimestamp := int64(1724350000000)
	upperOffset := int64(105)
	upperTimestamp := int64(1724360000000)
	expectedTimestamp := lowerTimestamp + ((commitedOffset - lowerOffset) * (upperTimestamp - lowerTimestamp) / (upperOffset - lowerOffset))
	expectedLagInSeconds := (latestProducedOffsetAt - expectedTimestamp) / 1000

	// Assert the expected lag metrics
	partition := group.Topics[0].Partitions[0]
	assert.Equal(t, expectedLagInOffsets, partition.LagInOffsets, "LagInOffsets should be calculated correctly")
	assert.Equal(t, expectedLagInSeconds, partition.LagInSeconds, "LagInSeconds should be interpolated correctly")
}

func TestMaxLagAtGroupLevel(t *testing.T) {
	// Create a LagProcessor instance
	lp := NewLagProcessor()

	// Set up partitions for two topics
	partitionsTopic1 := []structs.Partition{
		{
			Number:                 0,
			CommitedOffset:         100,
			LatestProducedOffset:   -1,
			LatestProducedOffsetAt: -1,
			LagInOffsets:           -1,
			LagInSeconds:           -1,
		},
		{
			Number:                 1,
			CommitedOffset:         180,
			LatestProducedOffset:   250,
			LatestProducedOffsetAt: 1724361000000,
			LagInOffsets:           70,
			LagInSeconds:           300,
		},
	}

	partitionsTopic2 := []structs.Partition{
		{
			Number:                 0,
			CommitedOffset:         300,
			LatestProducedOffset:   330,
			LatestProducedOffsetAt: 1724362000000,
			LagInOffsets:           30,
			LagInSeconds:           -1,
		},
		{
			Number:                 1,
			CommitedOffset:         400,
			LatestProducedOffset:   420,
			LatestProducedOffsetAt: 1724363000000,
			LagInOffsets:           20,
			LagInSeconds:           500,
		},
	}

	// Create the topics
	topic1 := structs.Topic{
		Name:       "test-topic-1",
		Partitions: partitionsTopic1,
	}
	topic2 := structs.Topic{
		Name:       "test-topic-2",
		Partitions: partitionsTopic2,
	}

	// Create the group with the two topics
	group := &structs.Group{
		Name:   "test-group",
		Topics: []structs.Topic{topic1, topic2},
	}

	// Process the group to calculate lag metrics
	lp.processGroup(group)

	// Expected max lag values at the group level (across all topics)
	expectedMaxLagInOffsets := int64(70)  // From topic1 partition1
	expectedMaxLagInSeconds := int64(500) // From topic1 partition1

	// Assert the max lag at the group level
	assert.Equal(t, expectedMaxLagInOffsets, group.MaxLagInOffsets, "MaxLagInOffsets should be correctly calculated at the group level")
	assert.Equal(t, expectedMaxLagInSeconds, group.MaxLagInSeconds, "MaxLagInSeconds should be correctly calculated at the group level")
}

func TestMaxLagAtTopicLevel(t *testing.T) {
	// Create a LagProcessor instance
	lp := NewLagProcessor()

	// Set up partitions for two topics
	partitionsTopic1 := []structs.Partition{
		{
			Number:                 0,
			CommitedOffset:         100,
			LatestProducedOffset:   110,
			LatestProducedOffsetAt: 1724360000000,
			LagInOffsets:           10,
			LagInSeconds:           -1,
		},
		{
			Number:                 1,
			CommitedOffset:         200,
			LatestProducedOffset:   250,
			LatestProducedOffsetAt: 1724361000000,
			LagInOffsets:           50,
			LagInSeconds:           500,
		},
	}

	partitionsTopic2 := []structs.Partition{
		{
			Number:                 0,
			CommitedOffset:         300,
			LatestProducedOffset:   330,
			LatestProducedOffsetAt: 1724362000000,
			LagInOffsets:           30,
			LagInSeconds:           300,
		},
		{
			Number:                 1,
			CommitedOffset:         400,
			LatestProducedOffset:   420,
			LatestProducedOffsetAt: 1724363000000,
			LagInOffsets:           20,
			LagInSeconds:           200,
		},
	}

	// Create the topics
	topic1 := structs.Topic{
		Name:       "test-topic-1",
		Partitions: partitionsTopic1,
	}
	topic2 := structs.Topic{
		Name:       "test-topic-2",
		Partitions: partitionsTopic2,
	}

	// Create the group with the two topics
	group := &structs.Group{
		Name:   "test-group",
		Topics: []structs.Topic{topic1, topic2},
	}

	// Process the group to calculate lag metrics
	lp.processGroup(group)

	// Expected max lag values for each topic
	expectedMaxLagInOffsetsTopic1 := int64(50)  // From partition1
	expectedMaxLagInSecondsTopic1 := int64(500) // From partition1
	expectedMaxLagInOffsetsTopic2 := int64(30)  // From partition0
	expectedMaxLagInSecondsTopic2 := int64(300) // From partition0

	// Assert the max lag at the topic level
	assert.Equal(t, expectedMaxLagInOffsetsTopic1, group.Topics[0].MaxLagInOffsets, "MaxLagInOffsets should be correctly calculated for topic 1")
	assert.Equal(t, expectedMaxLagInSecondsTopic1, group.Topics[0].MaxLagInSeconds, "MaxLagInSeconds should be correctly calculated for topic 1")
	assert.Equal(t, expectedMaxLagInOffsetsTopic2, group.Topics[1].MaxLagInOffsets, "MaxLagInOffsets should be correctly calculated for topic 2")
	assert.Equal(t, expectedMaxLagInSecondsTopic2, group.Topics[1].MaxLagInSeconds, "MaxLagInSeconds should be correctly calculated for topic 2")
}

// TestSumLagAtGroupLevel verifies that the sum of lag metrics is correctly calculated at the group level.
func TestSumLagAtGroupLevel(t *testing.T) {
	// Create a LagProcessor instance
	lp := NewLagProcessor()

	// Set up partitions for two topics
	partitionsTopic1 := []structs.Partition{
		{
			Number:                 0,
			CommitedOffset:         100,
			LatestProducedOffset:   110,
			LatestProducedOffsetAt: 1724360000000,
			LagInOffsets:           10,
			LagInSeconds:           100,
		},
		{
			Number:                 1,
			CommitedOffset:         200,
			LatestProducedOffset:   250,
			LatestProducedOffsetAt: 1724361000000,
			LagInOffsets:           50,
			LagInSeconds:           500,
		},
	}

	partitionsTopic2 := []structs.Partition{
		{
			Number:                 0,
			CommitedOffset:         300,
			LatestProducedOffset:   330,
			LatestProducedOffsetAt: 1724362000000,
			LagInOffsets:           30,
			LagInSeconds:           300,
		},
		{
			Number:                 1,
			CommitedOffset:         400,
			LatestProducedOffset:   420,
			LatestProducedOffsetAt: 1724363000000,
			LagInOffsets:           20,
			LagInSeconds:           200,
		},
	}

	// Create the topics
	topic1 := structs.Topic{
		Name:       "test-topic-1",
		Partitions: partitionsTopic1,
	}
	topic2 := structs.Topic{
		Name:       "test-topic-2",
		Partitions: partitionsTopic2,
	}

	// Create the group with the two topics
	group := &structs.Group{
		Name:   "test-group",
		Topics: []structs.Topic{topic1, topic2},
	}

	// Process the group to calculate lag metrics
	lp.processGroup(group)

	// Expected sum lag values at the group level
	expectedSumLagInOffsets := int64(110)  // Sum of lags from all partitions
	expectedSumLagInSeconds := int64(1100) // Sum of time lags from all partitions

	// Assert the sum lag at the group level
	assert.Equal(t, expectedSumLagInOffsets, group.SumLagInOffsets, "SumLagInOffsets should be correctly calculated at the group level")
	assert.Equal(t, expectedSumLagInSeconds, group.SumLagInSeconds, "SumLagInSeconds should be correctly calculated at the group level")
}

// TestSumLagAtTopicLevel verifies that the sum of lag metrics is correctly calculated at the topic level.
func TestSumLagAtTopicLevel(t *testing.T) {
	// Create a LagProcessor instance
	lp := NewLagProcessor()

	// Set up partitions for two topics
	partitionsTopic1 := []structs.Partition{
		{
			Number:                 0,
			CommitedOffset:         100,
			LatestProducedOffset:   110,
			LatestProducedOffsetAt: 1724360000000,
			LagInOffsets:           10,
			LagInSeconds:           100,
		},
		{
			Number:                 1,
			CommitedOffset:         200,
			LatestProducedOffset:   250,
			LatestProducedOffsetAt: 1724361000000,
			LagInOffsets:           50,
			LagInSeconds:           500,
		},
	}

	partitionsTopic2 := []structs.Partition{
		{
			Number:                 0,
			CommitedOffset:         300,
			LatestProducedOffset:   330,
			LatestProducedOffsetAt: 1724362000000,
			LagInOffsets:           30,
			LagInSeconds:           300,
		},
		{
			Number:                 1,
			CommitedOffset:         -1,
			LatestProducedOffset:   420,
			LatestProducedOffsetAt: 1724363000000,
			LagInOffsets:           -1,
			LagInSeconds:           -1,
		},
	}

	// Create the topics
	topic1 := structs.Topic{
		Name:       "test-topic-1",
		Partitions: partitionsTopic1,
	}
	topic2 := structs.Topic{
		Name:       "test-topic-2",
		Partitions: partitionsTopic2,
	}

	// Create the group with the two topics
	group := &structs.Group{
		Name:   "test-group",
		Topics: []structs.Topic{topic1, topic2},
	}

	// Process the group to calculate lag metrics
	lp.processGroup(group)

	// Expected sum lag values for each topic
	expectedSumLagInOffsetsTopic1 := int64(60)  // Sum of lags from all partitions in topic 1
	expectedSumLagInSecondsTopic1 := int64(600) // Sum of time lags from all partitions in topic 1
	expectedSumLagInOffsetsTopic2 := int64(30)  // Sum of lags from all partitions in topic 2
	expectedSumLagInSecondsTopic2 := int64(300) // Sum of time lags from all partitions in topic 2

	// Assert the sum lag at the topic level
	assert.Equal(t, expectedSumLagInOffsetsTopic1, group.Topics[0].SumLagInOffsets, "SumLagInOffsets should be correctly calculated for topic 1")
	assert.Equal(t, expectedSumLagInSecondsTopic1, group.Topics[0].SumLagInSeconds, "SumLagInSeconds should be correctly calculated for topic 1")
	assert.Equal(t, expectedSumLagInOffsetsTopic2, group.Topics[1].SumLagInOffsets, "SumLagInOffsets should be correctly calculated for topic 2")
	assert.Equal(t, expectedSumLagInSecondsTopic2, group.Topics[1].SumLagInSeconds, "SumLagInSeconds should be correctly calculated for topic 2")
}
