package storage

import (
	"context"
	"time"

	"github.com/sciclon2/kafka-lag-go/pkg/structs"
	"github.com/stretchr/testify/mock"
)

// MockStorage is a mock implementation of the Storage interface for testing purposes.
type MockStorage struct {
	mock.Mock
}

// RegisterNode mocks the RegisterNode method of the Storage interface.
func (m *MockStorage) RegisterNode(nodeID string, ttl int) (int, error) {
	args := m.Called(nodeID, ttl)
	return args.Int(0), args.Error(1)
}

// DeregisterNode mocks the DeregisterNode method of the Storage interface.
func (m *MockStorage) DeregisterNode(nodeID string) error {
	args := m.Called(nodeID)
	return args.Error(0)
}

// GetNodeInfo mocks the GetNodeInfo method of the Storage interface with the correct signature.
func (m *MockStorage) GetNodeInfo(nodeID string) (int, int, error) {
	args := m.Called(nodeID)
	return args.Int(0), args.Int(1), args.Error(2)
}

// StartNodeHeartbeat mocks the StartNodeHeartbeat method of the Storage interface.
func (m *MockStorage) StartNodeHeartbeat(nodeID string, heartbeatInterval time.Duration, ttl int) {
	m.Called(nodeID, heartbeatInterval, ttl)
}

// StartNodeMonitoring mocks the StartNodeMonitoring method of the Storage interface.
func (m *MockStorage) StartNodeMonitoring(monitorInterval time.Duration) {
	m.Called(monitorInterval)
}

// PersistLatestProducedOffsets mocks the PersistLatestProducedOffsets method of the Storage interface.
func (m *MockStorage) PersistLatestProducedOffsets(groupStructCompleteChan <-chan *structs.Group, numWorkers int) <-chan *structs.Group {
	m.Called(groupStructCompleteChan, numWorkers)
	mockReturnChan := make(chan *structs.Group)
	return mockReturnChan
}

// GracefulStop mocks the GracefulStop method of the Storage interface without the context argument.
func (m *MockStorage) GracefulStop() error {
	args := m.Called()
	return args.Error(0)
}

// Ping mocks the Ping method of the Storage interface. Adjusted to handle variadic context arguments.
func (m *MockStorage) Ping(ctx ...context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}
