package structs

import (
	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
)

// KafkaClient is an interface that wraps sarama.Client methods.
type KafkaClient interface {
	//Brokers() []KafkaBrokerInterface
	Brokers() []*sarama.Broker
	GetOffset(topic string, partition int32, time int64) (int64, error)
	RefreshMetadata(topics ...string) error
	Close() error
}

// KafkaAdmin is an interface that wraps sarama.ClusterAdmin methods.
type KafkaAdmin interface {
	ListConsumerGroups() (map[string]string, error)
	DescribeTopics(topics []string) ([]*sarama.TopicMetadata, error)
	ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error)
	ListTopics() (map[string]sarama.TopicDetail, error) // New method to list topics
	Close() error
}

type Group struct {
	Name         string         // Name of the consumer group
	ClusterName  string         // Name of the cluster
	Admin        KafkaAdmin     // Interface to the Kafka admin client
	Client       KafkaClient    // Interface to the Kafka client
	SaramaConfig *sarama.Config // Sarama configuration for the client

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
	Number                 int32          // Partition number
	CommitedOffset         int64          // Committed offset for this partition
	LatestProducedOffset   int64          // Latest produced offset for this partition. A value of 0 can signify that the value has not yet been fetched or is unavailable.
	LatestProducedOffsetAt int64          // Timestamp (in milliseconds) when the latest produced offset was set. A value of 0 can signify that it has not been set.
	ProducedOffsetsHistory []redis.Z      // Store the raw ZRANGE result (timestamps and scores). An empty slice can signify no history available.
	LeaderBroker           *sarama.Broker // Change to *sarama.Broker for consistency with Sarama API

	LagInOffsets int64 // Calculated offset lag. A value of 0 can be used to signify that it has not yet been calculated or there's no lag.
	LagInSeconds int64 // Calculated time lag in seconds. A value of 0 can be used to signify that it has not yet been calculated or there's no lag.
}

// HealthStatus represents the structure of the health check response
type HealthStatus struct {
	Status            string   `json:"status"`
	UnhealthyClusters []string `json:"unhealthy_clusters"`
}

// KafkaBrokerInterface abstracts sarama.Broker for easier testing
type KafkaBrokerInterface interface {
	Addr() string
	ID() int32
	Open(config *sarama.Config) error
	GetAvailableOffsets(req *sarama.OffsetRequest) (*sarama.OffsetResponse, error)
	Connected() (bool, error)
}

// SaramaKafkaClient wraps sarama.Client and implements the KafkaClient interface.
type SaramaKafkaClient struct {
	Client sarama.Client
}

// Brokers implements KafkaClient.Brokers by wrapping sarama.Broker.
// Brokers returns []*sarama.Broker to comply with Sarama's API
func (s *SaramaKafkaClient) Brokers() []*sarama.Broker {
	return s.Client.Brokers()
}

// Close closes the sarama.Client and implements the Close method for KafkaClient.
func (s *SaramaKafkaClient) Close() error {
	return s.Client.Close()
}

// Implement the missing GetOffset method
func (s *SaramaKafkaClient) GetOffset(topic string, partition int32, time int64) (int64, error) {
	return s.Client.GetOffset(topic, partition, time)
}

// Implement the missing RefreshMetadata method
func (s *SaramaKafkaClient) RefreshMetadata(topics ...string) error {
	return s.Client.RefreshMetadata(topics...)
}

// ... implement the rest of the methods required by KafkaClient

// SaramaKafkaAdmin wraps sarama.ClusterAdmin and implements the KafkaAdmin interface.
// SaramaKafkaAdmin wraps sarama.ClusterAdmin and implements the KafkaAdmin interface.
type SaramaKafkaAdmin struct {
	Admin sarama.ClusterAdmin
}

// ListConsumerGroups implements KafkaAdmin.ListConsumerGroups
func (s *SaramaKafkaAdmin) ListConsumerGroups() (map[string]string, error) {
	return s.Admin.ListConsumerGroups()
}

// DescribeTopics implements KafkaAdmin.DescribeTopics
func (s *SaramaKafkaAdmin) DescribeTopics(topics []string) ([]*sarama.TopicMetadata, error) {
	return s.Admin.DescribeTopics(topics)
}

// ListConsumerGroupOffsets implements KafkaAdmin.ListConsumerGroupOffsets
func (s *SaramaKafkaAdmin) ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	return s.Admin.ListConsumerGroupOffsets(group, topicPartitions)
}

// ListTopics implements KafkaAdmin.ListTopics
func (s *SaramaKafkaAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	return s.Admin.ListTopics()
}

// Close implements KafkaAdmin.Close
func (s *SaramaKafkaAdmin) Close() error {
	return s.Admin.Close()
}

// ... implement the rest of the methods required by KafkaAdmin

// SaramaBrokerWrapper wraps sarama.Broker and implements KafkaBrokerInterface.
type SaramaBrokerWrapper struct {
	broker *sarama.Broker
}

func (s *SaramaBrokerWrapper) ID() int32 {
	return s.broker.ID()
}

func (s *SaramaBrokerWrapper) Addr() string {
	return s.broker.Addr()
}

// Implement the Connected method
func (s *SaramaBrokerWrapper) Connected() (bool, error) {
	return s.broker.Connected()
}

// Implement the Open method
func (s *SaramaBrokerWrapper) Open(config *sarama.Config) error {
	return s.broker.Open(config)
}

// Implement the GetAvailableOffsets method
func (s *SaramaBrokerWrapper) GetAvailableOffsets(req *sarama.OffsetRequest) (*sarama.OffsetResponse, error) {
	return s.broker.GetAvailableOffsets(req)
}
