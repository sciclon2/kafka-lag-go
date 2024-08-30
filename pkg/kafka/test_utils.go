package kafka

import (
	"regexp"

	"github.com/IBM/sarama"
	"github.com/sciclon2/kafka-lag-go/pkg/config"
	"github.com/stretchr/testify/mock"
)

// MockSaramaBroker mocks the BrokerInterface for testing.
type MockSaramaBroker struct {
	mock.Mock
}

func (m *MockSaramaBroker) Brokers() []*sarama.Broker {
	args := m.Called()
	return args.Get(0).([]*sarama.Broker)
}

func (m *MockSaramaBroker) Topics() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockSaramaClusterAdmin) ListConsumerGroups() (map[string]string, error) {
	args := m.Called()
	return args.Get(0).(map[string]string), args.Error(1)
}

// Implement the BrokerInterface methods as needed
func (m *MockSaramaBroker) Connected() (bool, error) {
	args := m.Called()
	return args.Bool(0), args.Error(1)
}

func (m *MockSaramaBroker) Open(config *sarama.Config) error {
	args := m.Called(config)
	return args.Error(0)
}

func (m *MockSaramaBroker) GetAvailableOffsets(req *sarama.OffsetRequest) (*sarama.OffsetResponse, error) {
	args := m.Called(req)
	return args.Get(0).(*sarama.OffsetResponse), args.Error(1)
}

func (m *MockSaramaBroker) Addr() string {
	args := m.Called()
	return args.String(0)
}

// MockSaramaClusterAdmin mocks the sarama.ClusterAdmin interface for testing.
type MockSaramaClusterAdmin struct {
	mock.Mock
}

func (m *MockSaramaClusterAdmin) DescribeTopics(topics []string) ([]*sarama.TopicMetadata, error) {
	args := m.Called(topics)
	return args.Get(0).([]*sarama.TopicMetadata), args.Error(1)
}

func (m *MockSaramaClusterAdmin) ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	args := m.Called(group, topicPartitions)
	return args.Get(0).(*sarama.OffsetFetchResponse), args.Error(1)
}

// Implement the Close method to satisfy the KafkaAdmin interface
func (m *MockSaramaClusterAdmin) Close() error {
	args := m.Called()
	return args.Error(0)
}

// ListTopics mocks the ListTopics method for testing.
func (m *MockSaramaClusterAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	args := m.Called()
	return args.Get(0).(map[string]sarama.TopicDetail), args.Error(1)
}

// Helper function to mock ListConsumerGroups and return the channel for group IDs.
func setupMockAdminAndGroupChan(mockAdmin *MockSaramaClusterAdmin, groups map[string]string, bufferSize int) chan string {
	mockAdmin.On("ListConsumerGroups").Return(groups, nil)
	groupChan := make(chan string, bufferSize)
	return groupChan
}

// Helper function to create a basic config with optional whitelist and blacklist.
func createConfig(whitelist, blacklist string) *config.Config {
	conf := &config.Config{
		Kafka: struct {
			Brokers              []string `yaml:"brokers"`
			ClientRequestTimeout string   `yaml:"client_request_timeout"`
			MetadataFetchTimeout string   `yaml:"metadata_fetch_timeout"`
			ConsumerGroups       struct {
				Whitelist *regexp.Regexp `yaml:"whitelist"`
				Blacklist *regexp.Regexp `yaml:"blacklist"`
			} `yaml:"consumer_groups"`
			SSL struct {
				Enabled               bool   `yaml:"enabled"`
				ClientCertificateFile string `yaml:"client_certificate_file"`
				ClientKeyFile         string `yaml:"client_key_file"`
				InsecureSkipVerify    bool   `yaml:"insecure_skip_verify"`
			} `yaml:"ssl"`
			SASL struct {
				Enabled   bool   `yaml:"enabled"`
				Mechanism string `yaml:"mechanism"`
				User      string `yaml:"user"`
				Password  string `yaml:"password"`
			} `yaml:"sasl"`
		}{},
	}

	if whitelist != "" {
		conf.Kafka.ConsumerGroups.Whitelist = regexp.MustCompile(whitelist)
	}
	if blacklist != "" {
		conf.Kafka.ConsumerGroups.Blacklist = regexp.MustCompile(blacklist)
	}

	return conf
}
