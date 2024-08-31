package heartbeat

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/sciclon2/kafka-lag-go/pkg/kafka"
	"github.com/sciclon2/kafka-lag-go/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestApplicationHeartbeat(t *testing.T) {
	mockKafkaAdmin := new(kafka.MockSaramaClusterAdmin)
	mockStorage := new(storage.MockStorage)

	// Setup Kafka Admin mock
	mockKafkaAdmin.On("ListTopics").Return(map[string]sarama.TopicDetail{}, nil)

	// Setup Storage mock
	mockStorage.On("Ping", mock.Anything).Return(nil)

	// Create ApplicationHeartbeat instance
	ah := NewApplicationHeartbeat(mockKafkaAdmin, mockStorage, 1*time.Second, 8080, "/healthz")

	// Test Kafka check
	assert.True(t, ah.checkKafka())

	// Test Redis (Storage) check
	assert.True(t, ah.checkRedis(context.Background()))

	// Clean up
	mockKafkaAdmin.AssertExpectations(t)
	mockStorage.AssertExpectations(t)
}

func TestApplicationHeartbeat_KafkaFailure(t *testing.T) {
	mockKafkaAdmin := new(kafka.MockSaramaClusterAdmin)
	mockStorage := new(storage.MockStorage)

	// Setup Kafka Admin mock to simulate failure
	mockKafkaAdmin.On("ListTopics").Return(map[string]sarama.TopicDetail{}, fmt.Errorf("Kafka failure"))

	// Setup Storage mock
	mockStorage.On("Ping", mock.Anything).Return(nil)

	// Create ApplicationHeartbeat instance
	ah := NewApplicationHeartbeat(mockKafkaAdmin, mockStorage, 1*time.Second, 8080, "/healthz")

	// Test Kafka failure
	assert.False(t, ah.checkKafka())

	// Test Redis (Storage) check (should still pass)
	assert.True(t, ah.checkRedis(context.Background()))

	// Clean up
	mockKafkaAdmin.AssertExpectations(t)
	mockStorage.AssertExpectations(t)
}

func TestApplicationHeartbeat_RedisFailure(t *testing.T) {
	mockKafkaAdmin := new(kafka.MockSaramaClusterAdmin)
	mockStorage := new(storage.MockStorage)

	// Setup Kafka Admin mock to succeed
	mockKafkaAdmin.On("ListTopics").Return(map[string]sarama.TopicDetail{}, nil)

	// Setup Storage mock to fail
	mockStorage.On("Ping", mock.Anything).Return(fmt.Errorf("Redis failure"))

	// Create ApplicationHeartbeat instance
	ah := NewApplicationHeartbeat(mockKafkaAdmin, mockStorage, 1*time.Second, 8080, "/healthz")

	// Test Kafka check (should pass)
	assert.True(t, ah.checkKafka())

	// Test Redis (Storage) check (should fail)
	assert.False(t, ah.checkRedis(context.Background()))

	// Clean up
	mockKafkaAdmin.AssertExpectations(t)
	mockStorage.AssertExpectations(t)
}
func TestHealthCheckHandler(t *testing.T) {
	mockKafkaAdmin := new(kafka.MockSaramaClusterAdmin)
	mockStorage := new(storage.MockStorage)

	// Create ApplicationHeartbeat instance
	ah := NewApplicationHeartbeat(mockKafkaAdmin, mockStorage, 1*time.Second, 8080, "/healthz")

	tests := []struct {
		name           string
		status         bool
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "Healthy",
			status:         true,
			expectedStatus: http.StatusOK,
			expectedBody:   "OK",
		},
		{
			name:           "Unhealthy",
			status:         false,
			expectedStatus: http.StatusServiceUnavailable,
			expectedBody:   "Unhealthy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set the application status
			ah.StatusLock.Lock()
			ah.Status = tt.status
			ah.StatusLock.Unlock()

			// Test the HTTP handler
			req, _ := http.NewRequest("GET", "/healthz", nil)
			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(ah.HealthCheckHandler)
			handler.ServeHTTP(rr, req)

			// Verify the response
			assert.Equal(t, tt.expectedStatus, rr.Code)
			assert.Equal(t, tt.expectedBody, rr.Body.String())
		})
	}
}
