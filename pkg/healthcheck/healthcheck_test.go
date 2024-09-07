package healthcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/IBM/sarama"
	"github.com/sciclon2/kafka-lag-go/pkg/kafka"
	"github.com/sciclon2/kafka-lag-go/pkg/storage"
	"github.com/sciclon2/kafka-lag-go/pkg/structs"
)

func TestHealthCheckHandler_Healthy(t *testing.T) {
	// Setup mocks
	mockKafkaAdmin := new(kafka.MockKafkaAdmin)
	mockStorage := new(storage.MockStorage)

	// Simulate healthy Kafka and Storage
	mockKafkaAdmin.On("ListTopics").Return(map[string]sarama.TopicDetail{}, nil)
	mockStorage.On("Ping", mock.Anything).Return(nil)

	kafkaAdmins := map[string]structs.KafkaAdmin{"cluster-1": mockKafkaAdmin}

	// Create a new ApplicationHealthcheck instance
	ah := NewApplicationHealthcheck(kafkaAdmins, mockStorage, time.Second, 8080, "/health")

	// Simulate healthy status by running Kafka and Redis checks
	kafkaHealthy := ah.checkKafka()
	redisHealthy := ah.checkRedis(context.Background())

	ah.StatusLock.Lock()
	ah.Status = kafkaHealthy && redisHealthy
	ah.StatusLock.Unlock()

	// Create a request to the health check handler
	req, err := http.NewRequest("GET", "/health", nil)
	assert.NoError(t, err)

	// Record the response
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(ah.HealthCheckHandler)
	handler.ServeHTTP(rr, req)

	// Check that the status code is 200 OK
	assert.Equal(t, http.StatusOK, rr.Code)

	// Decode the JSON response
	var response structs.HealthStatus
	err = json.NewDecoder(rr.Body).Decode(&response)
	assert.NoError(t, err)

	// The status should be "OK" since both Kafka and Redis are healthy
	assert.Equal(t, "OK", response.Status)

	// The list of unhealthy clusters should be empty
	assert.Empty(t, response.UnhealthyClusters)

	// Verify mock expectations
	mockKafkaAdmin.AssertExpectations(t)
	mockStorage.AssertExpectations(t)
}

func TestHealthCheckHandler_OneKafkaClusterDown_OneUp(t *testing.T) {
	// Setup mocks for two Kafka clusters
	mockKafkaAdmin1 := new(kafka.MockKafkaAdmin) // Healthy cluster
	mockKafkaAdmin2 := new(kafka.MockKafkaAdmin) // Unhealthy cluster
	mockStorage := new(storage.MockStorage)

	// Simulate healthy Kafka cluster-1
	mockKafkaAdmin1.On("ListTopics").Return(map[string]sarama.TopicDetail{}, nil)

	// Simulate unhealthy Kafka cluster-2
	mockKafkaAdmin2.On("ListTopics").Return(map[string]sarama.TopicDetail{}, fmt.Errorf("Kafka failure"))

	// Simulate healthy Redis storage
	mockStorage.On("Ping", mock.Anything).Return(nil)

	kafkaAdmins := map[string]structs.KafkaAdmin{
		"cluster-1": mockKafkaAdmin1,
		"cluster-2": mockKafkaAdmin2,
	}

	// Create a new ApplicationHealthcheck instance
	ah := NewApplicationHealthcheck(kafkaAdmins, mockStorage, time.Second, 8080, "/health")

	// Run the Kafka health check and Redis check
	kafkaHealthy := ah.checkKafka()
	redisHealthy := ah.checkRedis(context.Background())

	// Set the health status according to the updated policy (healthy if at least one Kafka cluster is healthy and Redis is healthy)
	ah.StatusLock.Lock()
	ah.Status = kafkaHealthy && redisHealthy
	ah.StatusLock.Unlock()

	// Create a request to the health check handler
	req, err := http.NewRequest("GET", "/health", nil)
	assert.NoError(t, err)

	// Record the response
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(ah.HealthCheckHandler)
	handler.ServeHTTP(rr, req)

	// Check that the status code is 200 OK
	assert.Equal(t, http.StatusOK, rr.Code)

	// Check the JSON response structure
	var response structs.HealthStatus
	err = json.NewDecoder(rr.Body).Decode(&response)
	assert.NoError(t, err)

	// Since one Kafka cluster is healthy and Redis is healthy, the status should be OK
	assert.Equal(t, "OK", response.Status)

	// Check that cluster-2 is marked as unhealthy
	assert.Equal(t, []string{"cluster-2"}, response.UnhealthyClusters)

	// Verify mock expectations
	mockKafkaAdmin1.AssertExpectations(t)
	mockKafkaAdmin2.AssertExpectations(t)
	mockStorage.AssertExpectations(t)
}

func TestCheckKafka_Unhealthy(t *testing.T) {
	// Setup mocks
	mockKafkaAdmin := new(kafka.MockKafkaAdmin)

	// Simulate Kafka failure
	mockKafkaAdmin.On("ListTopics").Return(map[string]sarama.TopicDetail{}, fmt.Errorf("Kafka failure"))

	kafkaAdmins := map[string]structs.KafkaAdmin{
		"cluster-1": mockKafkaAdmin,
	}

	// Create a new ApplicationHealthcheck instance
	ah := NewApplicationHealthcheck(kafkaAdmins, nil, time.Second, 8080, "/health")

	// Run the Kafka health check
	healthy := ah.checkKafka()

	// Verify the result
	assert.False(t, healthy)
	assert.Equal(t, []string{"cluster-1"}, ah.UnhealthyClusters)
	mockKafkaAdmin.AssertExpectations(t)
}

func TestCheckRedis_Healthy(t *testing.T) {
	// Setup mocks
	mockStorage := new(storage.MockStorage)
	ctx := context.Background()

	// Simulate healthy Redis response
	mockStorage.On("Ping", mock.Anything).Return(nil)

	// Create a new ApplicationHealthcheck instance
	ah := NewApplicationHealthcheck(nil, mockStorage, time.Second, 8080, "/health")

	// Run the Redis health check
	healthy := ah.checkRedis(ctx)

	// Verify the result
	assert.True(t, healthy)
	mockStorage.AssertExpectations(t)
}

func TestCheckRedis_Unhealthy(t *testing.T) {
	// Setup mocks
	mockStorage := new(storage.MockStorage)
	ctx := context.Background()

	// Simulate Redis failure
	mockStorage.On("Ping", mock.Anything).Return(fmt.Errorf("Redis is down"))

	// Create a new ApplicationHealthcheck instance
	ah := NewApplicationHealthcheck(nil, mockStorage, time.Second, 8080, "/health")

	// Run the Redis health check
	healthy := ah.checkRedis(ctx)

	// Verify the result
	assert.False(t, healthy)
	mockStorage.AssertExpectations(t)
}

func TestStart_Healthy(t *testing.T) {
	// Setup mocks
	mockKafkaAdmin := new(kafka.MockKafkaAdmin)
	mockStorage := new(storage.MockStorage)

	// Simulate healthy Kafka and Redis
	mockKafkaAdmin.On("ListTopics").Return(map[string]sarama.TopicDetail{}, nil)
	mockStorage.On("Ping", mock.Anything).Return(nil)

	kafkaAdmins := map[string]structs.KafkaAdmin{
		"cluster-1": mockKafkaAdmin,
	}

	// Create a new ApplicationHealthcheck instance
	ah := NewApplicationHealthcheck(kafkaAdmins, mockStorage, time.Second, 8080, "/health")

	// Run the health check logic in the background with a delay of 1 second
	go ah.Start(1 * time.Second)

	// Simulate a delay to allow the full delay to pass and checks to run
	time.Sleep(2*time.Second + 500*time.Millisecond)

	// Create a request to the health check handler
	req, err := http.NewRequest("GET", "/health", nil)
	assert.NoError(t, err)

	// Record the response
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(ah.HealthCheckHandler)
	handler.ServeHTTP(rr, req)

	// Check that the status code is 200 OK
	assert.Equal(t, http.StatusOK, rr.Code)

	// Decode the JSON response
	var response structs.HealthStatus
	err = json.NewDecoder(rr.Body).Decode(&response)
	assert.NoError(t, err)

	// Check that the status is "OK" since Kafka and Redis are healthy
	assert.Equal(t, "OK", response.Status)

	// The list of unhealthy clusters should be empty
	assert.Empty(t, response.UnhealthyClusters)

	// Cleanup mock expectations
	mockKafkaAdmin.AssertExpectations(t)
	mockStorage.AssertExpectations(t)
}
