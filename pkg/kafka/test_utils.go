package kafka

import (
	"reflect"
	"unsafe"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/mock"
)

// MockKafkaClient mocks the KafkaClient interface for testing.
type MockKafkaClient struct {
	mock.Mock
}

// Brokers mocks the Brokers method.
func (m *MockKafkaClient) Brokers() []*sarama.Broker {
	args := m.Called()
	return args.Get(0).([]*sarama.Broker)
}

func (m *MockKafkaClient) Broker(brokerID int32) (*sarama.Broker, error) {
	args := m.Called(brokerID)
	return args.Get(0).(*sarama.Broker), args.Error(1)
}

// Topics mocks the Topics method.
func (m *MockKafkaClient) Topics() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

// GetOffset mocks the GetOffset method.
func (m *MockKafkaClient) GetOffset(topic string, partition int32, time int64) (int64, error) {
	args := m.Called(topic, partition, time)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockKafkaClient) Leader(topic string, partition int32) (*sarama.Broker, error) {
	args := m.Called(topic, partition)
	return args.Get(0).(*sarama.Broker), args.Error(1)
}

// RefreshMetadata mocks the RefreshMetadata method.
func (m *MockKafkaClient) RefreshMetadata(topics ...string) error {
	args := m.Called(topics)
	return args.Error(0)
}

// Close mocks the Close method.
func (m *MockKafkaClient) Close() error {
	args := m.Called()
	return args.Error(0)
}
func (m *MockKafkaClient) Closed() bool {
	args := m.Called()
	return args.Bool(0)
}

// Config mocks the Config method from sarama.Client.
func (m *MockKafkaClient) Config() *sarama.Config {
	args := m.Called()
	return args.Get(0).(*sarama.Config)
}

// Controller mocks the Controller method from sarama.Client.
func (m *MockKafkaClient) Controller() (*sarama.Broker, error) {
	args := m.Called()
	return args.Get(0).(*sarama.Broker), args.Error(1)
}

// MockKafkaAdmin mocks the KafkaAdmin interface for testing.
type MockKafkaAdmin struct {
	mock.Mock
}

// ListConsumerGroups mocks the ListConsumerGroups method.
func (m *MockKafkaAdmin) ListConsumerGroups() (map[string]string, error) {
	args := m.Called()
	return args.Get(0).(map[string]string), args.Error(1)
}

// DescribeTopics mocks the DescribeTopics method.
func (m *MockKafkaAdmin) DescribeTopics(topics []string) ([]*sarama.TopicMetadata, error) {
	args := m.Called(topics)
	return args.Get(0).([]*sarama.TopicMetadata), args.Error(1)
}

// ListConsumerGroupOffsets mocks the ListConsumerGroupOffsets method.
func (m *MockKafkaAdmin) ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	args := m.Called(group, topicPartitions)
	return args.Get(0).(*sarama.OffsetFetchResponse), args.Error(1)
}

// ListTopics mocks the ListTopics method.
func (m *MockKafkaAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	args := m.Called()
	return args.Get(0).(map[string]sarama.TopicDetail), args.Error(1)
}

// Close mocks the Close method.
func (m *MockKafkaAdmin) Close() error {
	args := m.Called()
	return args.Error(0)
}

// CustomBroker wraps sarama.Broker and allows setting the Broker ID
type CustomBroker struct {
	*sarama.Broker
	brokerID         int32
	mockOffsetResult *sarama.OffsetResponse
	mockOffsetError  error
	mockConnected    bool
	mockOpenError    error
}

// Connected mocks the Connected method to return predefined connection status
func (b *CustomBroker) Connected() (bool, error) {
	return b.mockConnected, nil
}

// SetMockConnectionState allows setting the mock connection state
func (b *CustomBroker) SetMockConnectionState(connected bool) {
	b.mockConnected = connected
}

// Open mocks the Open method to prevent real network connections
func (b *CustomBroker) Open(config *sarama.Config) error {
	// You can simulate an error or return nil based on test requirements
	return b.mockOpenError
}

// ID overrides the sarama.Broker's ID method to return a custom ID
func (b *CustomBroker) ID() int32 {
	return b.brokerID
}

// SetMockOpenError allows setting a mock error for Open method
func (b *CustomBroker) SetMockOpenError(err error) {
	b.mockOpenError = err
}

// SetMockOffsetResponse sets a mock OffsetResponse to be returned by GetAvailableOffsets
func (b *CustomBroker) SetMockOffsetResponse(response *sarama.OffsetResponse, err error) {
	b.mockOffsetResult = response
	b.mockOffsetError = err
}

// GetAvailableOffsets mocks the GetAvailableOffsets method to return predefined responses
func (b *CustomBroker) GetAvailableOffsets(req *sarama.OffsetRequest) (*sarama.OffsetResponse, error) {
	return b.mockOffsetResult, b.mockOffsetError
}

// NewCustomBroker creates a new CustomBroker and sets the broker ID
func NewCustomBroker(addr string, id int32) *CustomBroker {
	broker := sarama.NewBroker(addr) // Create a sarama.Broker
	return &CustomBroker{
		Broker:   broker,
		brokerID: id, // Set the custom broker ID
	}
}

// Helper function to set the unexported 'id' field in sarama.Broker using unsafe
func setBrokerID(broker *sarama.Broker, id int32) {
	brokerValue := reflect.ValueOf(broker).Elem()
	idField := brokerValue.FieldByName("id")

	// Use unsafe to bypass reflect restrictions
	ptrToId := unsafe.Pointer(idField.UnsafeAddr())
	*(*int32)(ptrToId) = id
}
