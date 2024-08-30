package redis

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/stretchr/testify/assert"
)

func TestRegisterNode_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	nodeID := "node1"
	ttl := 60
	expectedIndex := int64(1)

	mockClient.On("EvalSha", ctx, "mockSHA", []string{"register_or_refresh"}, nodeID, ttl).
		Return(redis.NewCmdResult(expectedIndex, nil))

	manager := &RedisManager{
		client: mockClient,
		ctx:    ctx,
		LuaSHA: "mockSHA",
	}

	index, err := manager.RegisterNode(nodeID, ttl)
	assert.NoError(t, err)
	assert.Equal(t, int(expectedIndex), index)

	mockClient.AssertExpectations(t)
}

func TestRegisterNode_LuaScriptError(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	nodeID := "node1"
	ttl := 60
	expectedError := "Error Lua"

	// Mock the EvalSha to return an error
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"register_or_refresh"}, nodeID, ttl).
		Return(redis.NewCmdResult(nil, fmt.Errorf(expectedError)))

	manager := &RedisManager{
		client: mockClient,
		ctx:    ctx,
		LuaSHA: "mockSHA",
	}

	index, err := manager.RegisterNode(nodeID, ttl)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError) // Check if the error contains the expected message
	assert.Equal(t, -1, index)

	mockClient.AssertExpectations(t)
}

func TestRegisterNode_UnexpectedResultFormat(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	nodeID := "node1"
	ttl := 60

	// Mock the EvalSha to return a result in an unexpected format (e.g., string)
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"register_or_refresh"}, nodeID, ttl).
		Return(redis.NewCmdResult("unexpected_format", nil))

	manager := &RedisManager{
		client: mockClient,
		ctx:    ctx,
		LuaSHA: "mockSHA",
	}

	index, err := manager.RegisterNode(nodeID, ttl)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected result format")
	assert.Equal(t, -1, index)

	mockClient.AssertExpectations(t)
}

func TestDeregisterNode_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	nodeID := "node1"

	// Mock the EvalSha to simulate a successful deregistration
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"deregister"}, nodeID).
		Return(redis.NewCmdResult(nil, nil))

	manager := &RedisManager{
		client: mockClient,
		ctx:    ctx,
		LuaSHA: "mockSHA",
	}

	err := manager.DeregisterNode(nodeID)

	// Assertions
	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
}
func TestDeregisterNode_LuaScriptError(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	nodeID := "node1"
	expectedError := fmt.Errorf("Lua script execution error")

	// Mock the EvalSha to simulate an error during deregistration
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"deregister"}, nodeID).
		Return(redis.NewCmdResult(nil, expectedError))

	manager := &RedisManager{
		client: mockClient,
		ctx:    ctx,
		LuaSHA: "mockSHA",
	}

	err := manager.DeregisterNode(nodeID)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error executing Lua script for deregistration")
	assert.Contains(t, err.Error(), expectedError.Error())

	mockClient.AssertExpectations(t)
}

func TestGetNodeInfo_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	nodeID := "node1"
	expectedIndex := int64(1)
	expectedTotalNodes := int64(5)

	// Mock the EvalSha to return a successful result
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"get_node_info"}, nodeID).
		Return(redis.NewCmdResult([]interface{}{"ok", expectedIndex, expectedTotalNodes}, nil))

	manager := &RedisManager{
		client: mockClient,
		ctx:    ctx,
		LuaSHA: "mockSHA",
	}

	index, totalNodes, err := manager.GetNodeInfo(nodeID)

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, int(expectedIndex), index)
	assert.Equal(t, int(expectedTotalNodes), totalNodes)

	mockClient.AssertExpectations(t)
}
func TestGetNodeInfo_LuaScriptError(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	nodeID := "node1"
	expectedError := fmt.Errorf("Lua script error")

	// Mock the EvalSha to return an error
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"get_node_info"}, nodeID).
		Return(redis.NewCmdResult(nil, expectedError))

	manager := &RedisManager{
		client: mockClient,
		ctx:    ctx,
		LuaSHA: "mockSHA",
	}

	_, _, err := manager.GetNodeInfo(nodeID)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error executing Lua script for getting node info")
	assert.Contains(t, err.Error(), expectedError.Error())

	mockClient.AssertExpectations(t)
}

func TestGetNodeInfo_NodeNotFound(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	nodeID := "node1"

	// Mock the EvalSha to return "not_found" status
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"get_node_info"}, nodeID).
		Return(redis.NewCmdResult([]interface{}{"not_found"}, nil))

	manager := &RedisManager{
		client: mockClient,
		ctx:    ctx,
		LuaSHA: "mockSHA",
	}

	_, _, err := manager.GetNodeInfo(nodeID)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "node not found")

	mockClient.AssertExpectations(t)
}

func TestGetNodeInfo_UnexpectedResultFormat(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	nodeID := "node1"

	// Mock the EvalSha to return an unexpected result format
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"get_node_info"}, nodeID).
		Return(redis.NewCmdResult("unexpected_format", nil))

	manager := &RedisManager{
		client: mockClient,
		ctx:    ctx,
		LuaSHA: "mockSHA",
	}

	_, _, err := manager.GetNodeInfo(nodeID)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected result format")

	mockClient.AssertExpectations(t)
}

func TestStartNodeHeartbeat_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mockClient := createMockRedisClient()

	ttl := 60
	nodeID := "node1"
	heartbeatInterval := 100 * time.Millisecond

	// Mock the EvalSha method to simulate the Lua script execution for refreshing the node
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"register_or_refresh"}, nodeID, ttl).
		Return(redis.NewCmdResult("TTL refreshed", nil)).Twice()

	manager := &RedisManager{
		client:    mockClient,
		ctx:       ctx,
		cancelCtx: cancel,
		LuaSHA:    "mockSHA", // Use the mock SHA1 value here
	}

	// Start the heartbeat
	manager.StartNodeHeartbeat(nodeID, heartbeatInterval, ttl)

	// Wait a short amount of time, then cancel the context to simulate a shutdown
	time.Sleep(heartbeatInterval * 2)
	cancel()

	// Wait a moment to ensure the goroutine has exited
	time.Sleep(heartbeatInterval * 2)

	// The goroutine should have stopped, and no further calls should have been made to RefreshNode
	mockClient.AssertExpectations(t)
}

func TestStartNodeHeartbeat_RefreshNodeFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mockClient := createMockRedisClient()

	ttl := 60
	nodeID := "node1"
	heartbeatInterval := 100 * time.Millisecond

	// Mock the EvalSha method to simulate a failure in RefreshNode
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"register_or_refresh"}, nodeID, ttl).
		Return(redis.NewCmdResult("", fmt.Errorf("expire error"))).Maybe()

	manager := &RedisManager{
		client:    mockClient,
		ctx:       ctx,
		cancelCtx: cancel,
		LuaSHA:    "mockSHA", // Ensure LuaSHA is set correctly here
	}

	// Start the heartbeat
	manager.StartNodeHeartbeat(nodeID, heartbeatInterval, ttl)

	// Increase the wait time before canceling the context
	time.Sleep(heartbeatInterval * 2)
	cancel()

	// Wait a moment to ensure the goroutine has exited
	time.Sleep(heartbeatInterval * 2)

	// The goroutine should have stopped, and RefreshNode should have been called at least once with an error
	mockClient.AssertExpectations(t)
}

func TestStartNodeHeartbeat_NodeKeyDoesNotExist(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mockClient := createMockRedisClient()

	ttl := 60
	nodeID := "node1"
	heartbeatInterval := 100 * time.Millisecond

	// Mock the EvalSha method to simulate the key not existing initially and being created
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"register_or_refresh"}, nodeID, ttl).
		Return(redis.NewCmdResult("Key created and added to node list", nil)).Maybe()

	manager := &RedisManager{
		client:    mockClient,
		ctx:       ctx,
		cancelCtx: cancel,
		LuaSHA:    "mockSHA", // Ensure LuaSHA is set correctly here
	}

	// Start the heartbeat
	manager.StartNodeHeartbeat(nodeID, heartbeatInterval, ttl)

	// Wait a short amount of time, then cancel the context to simulate a shutdown
	time.Sleep(heartbeatInterval * 2)
	cancel()

	// Wait a moment to ensure the goroutine has exited
	time.Sleep(heartbeatInterval * 2)

	// The goroutine should have stopped, and RefreshNode should have been called with the key creation result
	mockClient.AssertExpectations(t)
}

// Test to verify that the monitoring logs the correct message when nodes fail
func TestStartNodeMonitoring_LogsFailedNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient := createMockRedisClient()

	// Mock EvalSha to simulate the Lua script for monitoring nodes
	// This should return a list of failed nodes, for example, "node1" and "node2"
	failedNodesResult := []interface{}{"node1", "node2"}
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"monitor"}).
		Return(redis.NewCmdResult(failedNodesResult, nil)).Once()

	// Initialize the RedisManager with the mock client
	manager := &RedisManager{
		client: mockClient,
		ctx:    ctx,
		LuaSHA: "mockSHA",
	}

	// Capture the log output and ensure it's reset after the test
	var logBuffer bytes.Buffer
	logrus.SetOutput(&logBuffer)
	logrus.SetLevel(logrus.DebugLevel) // Ensure that Debug level logs are captured
	defer logrus.SetOutput(os.Stderr)  // Restore original log output after test

	// Start node monitoring
	manager.StartNodeMonitoring(1 * time.Second)

	// Let it run for a short time
	time.Sleep(1500 * time.Millisecond)

	// Simulate context cancel
	cancel()

	// Ensure the goroutine has time to stop
	time.Sleep(100 * time.Millisecond)

	// Verify the mock method was called
	mockClient.AssertExpectations(t)

	// Check the log output for the expected debug message
	logOutput := logBuffer.String()
	assert.Contains(t, logOutput, "Removed failed nodes: [node1 node2]")
}

func TestStartNodeMonitoring_NoFailedNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient := createMockRedisClient()

	// Mock EvalSha to simulate the Lua script for monitoring nodes
	// This should return an empty list, indicating no failed nodes.
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"monitor"}).
		Return(redis.NewCmdResult([]interface{}{}, nil)).Once()

	// Initialize the RedisManager with the mock client
	manager := &RedisManager{
		client: mockClient,
		ctx:    ctx,
		LuaSHA: "mockSHA",
	}

	// Capture the log output
	var logBuffer bytes.Buffer
	log.SetOutput(&logBuffer)
	t.Cleanup(func() {
		log.SetOutput(nil)
	})

	// Start node monitoring
	manager.StartNodeMonitoring(1 * time.Second)

	// Let it run for a short time
	time.Sleep(1500 * time.Millisecond)

	// Simulate context cancel
	cancel()

	// Ensure the goroutine has time to stop
	time.Sleep(100 * time.Millisecond)

	// Verify the mock method was called
	mockClient.AssertExpectations(t)

	// Check the log output for absence of removed nodes message
	logOutput := logBuffer.String()
	assert.NotContains(t, logOutput, "Removed failed nodes")
}

func TestStartNodeMonitoring_LuaScriptError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient := createMockRedisClient()

	// Mock EvalSha to return an error, simulating a Lua script failure
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"monitor"}).
		Return(redis.NewCmdResult(nil, fmt.Errorf("Lua script error"))).Once()

	// Initialize the RedisManager with the mock client
	manager := &RedisManager{
		client: mockClient,
		ctx:    ctx,
		LuaSHA: "mockSHA",
	}

	// Capture the log output using logrus and ensure it's reset after the test
	var logBuffer bytes.Buffer
	logrus.SetOutput(&logBuffer)
	defer logrus.SetOutput(os.Stderr) // Restore original log output after the test

	// Start node monitoring
	manager.StartNodeMonitoring(1 * time.Second)

	// Let it run for a short time to allow the error to be logged
	time.Sleep(1500 * time.Millisecond)

	// Simulate context cancellation
	cancel()

	// Ensure the goroutine has time to stop
	time.Sleep(100 * time.Millisecond)

	// Verify the mock method was called
	mockClient.AssertExpectations(t)

	// Check the log output for the expected error message
	logOutput := logBuffer.String()
	assert.Contains(t, logOutput, "Failed to monitor nodes: error executing Lua script for monitor nodes: Lua script error")
}

func TestStartNodeMonitoring_ImmediateStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	mockClient := createMockRedisClient()

	// Since the context is canceled immediately, EvalSha should never be called.
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"monitor"}).Return(nil).Maybe()

	// Initialize the RedisManager with the mock client
	manager := &RedisManager{
		client: mockClient,
		ctx:    ctx,
		LuaSHA: "mockSHA",
	}

	// Capture the log output
	var logBuffer bytes.Buffer
	logrus.SetOutput(&logBuffer)
	defer logrus.SetOutput(os.Stderr) // Restore the original output

	// Start node monitoring
	manager.StartNodeMonitoring(1 * time.Second)

	// Immediately cancel the context
	cancel()

	// Ensure the goroutine has time to stop
	time.Sleep(100 * time.Millisecond)

	// Verify the mock method was not called
	mockClient.AssertExpectations(t)

	// Check the log output for the stop message
	logOutput := logBuffer.String()
	assert.Contains(t, logOutput, "Node monitoring stopped")
}

func TestStartNodeMonitoring_TickerExpiresTwice(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient := createMockRedisClient()

	// Mock EvalSha to simulate the Lua script for monitoring nodes
	// This will be called twice before stopping.
	failedNodesResult := []interface{}{"node1", "node2"}
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"monitor"}).
		Return(redis.NewCmdResult(failedNodesResult, nil)).Twice()

	// Initialize the RedisManager with the mock client
	manager := &RedisManager{
		client: mockClient,
		ctx:    ctx,
		LuaSHA: "mockSHA",
	}

	// Capture the log output using logrus and ensure it's reset after the test
	var logBuffer bytes.Buffer
	logrus.SetOutput(&logBuffer)
	defer logrus.SetOutput(os.Stderr) // Restore original log output after the test

	// Start node monitoring
	manager.StartNodeMonitoring(500 * time.Millisecond)

	// Let it run for a short time
	time.Sleep(1200 * time.Millisecond)

	// Simulate context cancel
	cancel()

	// Ensure the goroutine has time to stop
	time.Sleep(100 * time.Millisecond)

	// Verify the mock method was called twice
	mockClient.AssertExpectations(t)

	// Check the log output for the expected message
	logOutput := logBuffer.String()
	assert.Contains(t, logOutput, "Removed failed nodes: [node1 node2]")
}

func TestMonitorNodes_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	// Simulate the Lua script returning a valid list of failed nodes
	failedNodesResult := []interface{}{"node1", "node2"}
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"monitor"}).
		Return(redis.NewCmdResult(failedNodesResult, nil)).Once()

	manager := &RedisManager{
		client: mockClient,
		ctx:    ctx,
		LuaSHA: "mockSHA",
	}

	failedNodes, err := manager.monitorNodes()

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, []string{"node1", "node2"}, failedNodes)

	mockClient.AssertExpectations(t)
}

func TestMonitorNodes_LuaScriptError(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	// Simulate the Lua script failing
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"monitor"}).
		Return(redis.NewCmdResult(nil, fmt.Errorf("Lua script error"))).Once()

	manager := &RedisManager{
		client: mockClient,
		ctx:    ctx,
		LuaSHA: "mockSHA",
	}

	failedNodes, err := manager.monitorNodes()

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, failedNodes)
	assert.Contains(t, err.Error(), "error executing Lua script for monitor nodes: Lua script error")

	mockClient.AssertExpectations(t)
}
func TestMonitorNodes_InvalidNodeIDType(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	// Simulate the Lua script returning a list with an invalid node ID type (e.g., an integer instead of a string)
	invalidNodeIDResult := []interface{}{"node1", 12345}
	mockClient.On("EvalSha", ctx, "mockSHA", []string{"monitor"}).
		Return(redis.NewCmdResult(invalidNodeIDResult, nil)).Once()

	manager := &RedisManager{
		client: mockClient,
		ctx:    ctx,
		LuaSHA: "mockSHA",
	}

	failedNodes, err := manager.monitorNodes()

	// Assertions
	assert.Error(t, err)
	assert.Nil(t, failedNodes)
	assert.Contains(t, err.Error(), "unexpected node ID type: int")

	mockClient.AssertExpectations(t)
}
