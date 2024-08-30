package structs

import (
	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
)

type BrokerInterface interface {
	Connected() (bool, error)
	Open(config *sarama.Config) error
	GetAvailableOffsets(req *sarama.OffsetRequest) (*sarama.OffsetResponse, error)
	Addr() string
}

type Group struct {
	Name            string  // Name of the consumer group
	Topics          []Topic // List of topics in the consumer group
	MaxLagInOffsets int64   // Maximum lag in offsets for the entire group
	MaxLagInSeconds int64   // Maximum lag in seconds for the entire group
	SumLagInOffsets int64   // Sum of lag in offsets for the entire group
	SumLagInSeconds int64   // Sum of lag in seconds for the entire group
}

type Topic struct {
	Name            string      // Name of the topic
	Partitions      []Partition // List of partitions in the topic
	SumLagInOffsets int64       // Sum of lag in offsets for the topic
	SumLagInSeconds int64       // Sum of lag in seconds for the topic
	MaxLagInOffsets int64       // Maximum lag in offsets for the topic
	MaxLagInSeconds int64       // Maximum lag in seconds for the topic
}

type Partition struct {
	Number                 int32           // Partition number
	CommitedOffset         int64           // Committed offset for this partition
	LatestProducedOffset   int64           // Latest produced offset for this partition. A value of 0 can signify that the value has not yet been fetched or is unavailable.
	LatestProducedOffsetAt int64           // Timestamp (in milliseconds) when the latest produced offset was set. A value of 0 can signify that it has not been set.
	ProducedOffsetsHistory []redis.Z       // Store the raw ZRANGE result (timestamps and scores). An empty slice can signify no history available.
	LeaderBroker           BrokerInterface // Interface to the leader broker for this partition

	LagInOffsets int64 // Calculated offset lag. A value of 0 can be used to signify that it has not yet been calculated or there's no lag.
	LagInSeconds int64 // Calculated time lag in seconds. A value of 0 can be used to signify that it has not yet been calculated or there's no lag.
}
