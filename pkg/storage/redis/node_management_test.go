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

	nodeID := "node"
	ttl := 60
	expectedIndex := int64(1)

	// Update the nodeKey and nodeList to include the expected hash tag format
	nodeKey := "{node_tag}:node"
	nodeList := "{node_tag}:active_nodes"

	// Mock the EvalSha with the corrected arguments
	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeKey}, "register_or_refresh", nodeList, ttl).
		Return(redis.NewCmdResult(expectedIndex, nil))

	manager := &RedisManager{
		client:  mockClient,
		ctx:     ctx,
		LuaSHA:  "mockSHA",
		NodeTag: "node_tag",
	}

	index, err := manager.RegisterNode(nodeID, ttl)
	assert.NoError(t, err)
	assert.Equal(t, int(expectedIndex), index)

	mockClient.AssertExpectations(t)
}

func TestRegisterNode_LuaScriptError(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	nodeID := "node"
	ttl := 60
	expectedError := "Error Lua"

	// Update the nodeKey and nodeList to include the expected hash tag format
	nodeKey := "{node_tag}:node"
	nodeList := "{node_tag}:active_nodes"

	// Mock the EvalSha to return an error
	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeKey}, "register_or_refresh", nodeList, ttl).
		Return(redis.NewCmdResult(nil, fmt.Errorf(expectedError)))

	manager := &RedisManager{
		client:  mockClient,
		ctx:     ctx,
		LuaSHA:  "mockSHA",
		NodeTag: "node_tag",
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

	nodeID := "node"
	ttl := 60

	// Update the nodeKey and nodeList to include the expected hash tag format
	nodeKey := "{node_tag}:node"
	nodeList := "{node_tag}:active_nodes"

	// Mock the EvalSha to return a result in an unexpected format (e.g., string)
	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeKey}, "register_or_refresh", nodeList, ttl).
		Return(redis.NewCmdResult("unexpected_format", nil))

	manager := &RedisManager{
		client:  mockClient,
		ctx:     ctx,
		LuaSHA:  "mockSHA",
		NodeTag: "node_tag",
	}

	index, err := manager.RegisterNode(nodeID, ttl)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected result format")
	assert.Equal(t, -1, index)

	mockClient.AssertExpectations(t)
}

func TestDeregisterNode_LuaScriptError(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	nodeID := "node1"
	expectedError := fmt.Errorf("Lua script execution error")

	// Create nodeKey and nodeList as per the actual code structure
	nodeKey := "{node_tag}:node1"         // Ensure hash tag is present
	nodeList := "{node_tag}:active_nodes" // Ensure hash tag is present

	// Mock the EvalSha with the correct arguments
	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeKey}, "deregister", nodeList).
		Return(redis.NewCmdResult(nil, expectedError))

	manager := &RedisManager{
		client:  mockClient,
		ctx:     ctx,
		LuaSHA:  "mockSHA",
		NodeTag: "node_tag",
	}

	err := manager.DeregisterNode(nodeID)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error executing Lua script for deregistration")
	assert.Contains(t, err.Error(), expectedError.Error())

	mockClient.AssertExpectations(t)
}

func TestDeregisterNode_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	nodeID := "node"
	// Ensure nodeKey and nodeList use the hash tag
	nodeKey := "{node_tag}:node"
	nodeList := "{node_tag}:active_nodes"

	// Mock the EvalSha to simulate a successful deregistration
	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeKey}, "deregister", nodeList).
		Return(redis.NewCmdResult(nil, nil))

	manager := &RedisManager{
		client:  mockClient,
		ctx:     ctx,
		LuaSHA:  "mockSHA",
		NodeTag: "node_tag",
	}

	err := manager.DeregisterNode(nodeID)

	// Assertions
	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
}
func TestGetNodeInfo_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	nodeID := "node"
	expectedIndex := int64(1)
	expectedTotalNodes := int64(5)

	// Update the nodeKey and nodeList to include the expected hash tag format
	nodeKey := "{node_tag}:node"          // Ensure hash tag is present
	nodeList := "{node_tag}:active_nodes" // Ensure hash tag is present

	// Mock the EvalSha to return a successful result
	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeKey}, "get_node_info", nodeList).
		Return(redis.NewCmdResult([]interface{}{"ok", expectedIndex, expectedTotalNodes}, nil))

	manager := &RedisManager{
		client:  mockClient,
		ctx:     ctx,
		LuaSHA:  "mockSHA",
		NodeTag: "node_tag",
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

	nodeID := "node"
	expectedError := fmt.Errorf("Lua script error")

	nodeKey := "{node_tag}:node"
	nodeList := "{node_tag}:active_nodes"

	// Mock the EvalSha to return an error
	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeKey}, "get_node_info", nodeList).
		Return(redis.NewCmdResult(nil, expectedError))

	manager := &RedisManager{
		client:  mockClient,
		ctx:     ctx,
		LuaSHA:  "mockSHA",
		NodeTag: "node_tag",
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

	nodeID := "node"

	nodeKey := "{node_tag}:node"
	nodeList := "{node_tag}:active_nodes"

	// Mock the EvalSha to return "not_found" status
	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeKey}, "get_node_info", nodeList).
		Return(redis.NewCmdResult([]interface{}{"not_found"}, nil))

	manager := &RedisManager{
		client:  mockClient,
		ctx:     ctx,
		LuaSHA:  "mockSHA",
		NodeTag: "node_tag",
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

	nodeID := "node"

	nodeKey := "{node_tag}:node"
	nodeList := "{node_tag}:active_nodes"

	// Mock the EvalSha to return an unexpected result format
	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeKey}, "get_node_info", nodeList).
		Return(redis.NewCmdResult("unexpected_format", nil))

	manager := &RedisManager{
		client:  mockClient,
		ctx:     ctx,
		LuaSHA:  "mockSHA",
		NodeTag: "node_tag",
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

	nodeID := "node"
	ttl := 60
	heartbeatInterval := 100 * time.Millisecond

	nodeKey := "{node_tag}:node"
	nodeList := "{node_tag}:active_nodes"

	// Mock the EvalSha method to simulate the Lua script execution for refreshing the node
	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeKey}, "register_or_refresh", nodeList, ttl).
		Return(redis.NewCmdResult("TTL refreshed", nil)).Twice()

	manager := &RedisManager{
		client:    mockClient,
		ctx:       ctx,
		cancelCtx: cancel,
		LuaSHA:    "mockSHA",
		NodeTag:   "node_tag",
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

	nodeID := "node"
	ttl := 60
	heartbeatInterval := 100 * time.Millisecond

	nodeKey := "{node_tag}:node"
	nodeList := "{node_tag}:active_nodes"

	// Mock the EvalSha method to simulate a failure in RefreshNode
	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeKey}, "register_or_refresh", nodeList, ttl).
		Return(redis.NewCmdResult("", fmt.Errorf("expire error"))).Maybe()

	manager := &RedisManager{
		client:    mockClient,
		ctx:       ctx,
		cancelCtx: cancel,
		LuaSHA:    "mockSHA",
		NodeTag:   "node_tag",
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

func TestStartNodeMonitoring_LogsFailedNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient := createMockRedisClient()

	// Mock EvalSha to simulate the Lua script for monitoring nodes
	failedNodesResult := []interface{}{"node1", "node2"}
	nodeList := "{node_tag}:active_nodes"

	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeList}, "monitor").
		Return(redis.NewCmdResult(failedNodesResult, nil)).Once()

	manager := &RedisManager{
		client:  mockClient,
		ctx:     ctx,
		LuaSHA:  "mockSHA",
		NodeTag: "node_tag",
	}

	var logBuffer bytes.Buffer
	logrus.SetOutput(&logBuffer)
	logrus.SetLevel(logrus.DebugLevel)
	defer logrus.SetOutput(os.Stderr)

	manager.StartNodeMonitoring(1 * time.Second)

	time.Sleep(1500 * time.Millisecond)
	cancel()

	time.Sleep(100 * time.Millisecond)

	mockClient.AssertExpectations(t)

	logOutput := logBuffer.String()
	assert.Contains(t, logOutput, "Removed failed nodes: [node1 node2]")
}
func TestStartNodeMonitoring_NoFailedNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient := createMockRedisClient()

	nodeList := "{node_tag}:active_nodes"

	// Mock EvalSha to simulate the Lua script for monitoring nodes
	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeList}, "monitor").
		Return(redis.NewCmdResult([]interface{}{}, nil)).Once()

	manager := &RedisManager{
		client:  mockClient,
		ctx:     ctx,
		LuaSHA:  "mockSHA",
		NodeTag: "node_tag",
	}

	var logBuffer bytes.Buffer
	log.SetOutput(&logBuffer)
	t.Cleanup(func() {
		log.SetOutput(nil)
	})

	manager.StartNodeMonitoring(1 * time.Second)

	time.Sleep(1500 * time.Millisecond)
	cancel()

	time.Sleep(100 * time.Millisecond)

	mockClient.AssertExpectations(t)

	logOutput := logBuffer.String()
	assert.NotContains(t, logOutput, "Removed failed nodes")
}

func TestStartNodeMonitoring_LuaScriptError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient := createMockRedisClient()

	nodeList := "{node_tag}:active_nodes"

	// Mock EvalSha to return an error
	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeList}, "monitor").
		Return(redis.NewCmdResult(nil, fmt.Errorf("Lua script error"))).Once()

	manager := &RedisManager{
		client:  mockClient,
		ctx:     ctx,
		LuaSHA:  "mockSHA",
		NodeTag: "node_tag",
	}

	var logBuffer bytes.Buffer
	logrus.SetOutput(&logBuffer)
	defer logrus.SetOutput(os.Stderr)

	manager.StartNodeMonitoring(1 * time.Second)

	time.Sleep(1500 * time.Millisecond)
	cancel()

	time.Sleep(100 * time.Millisecond)

	mockClient.AssertExpectations(t)

	logOutput := logBuffer.String()
	assert.Contains(t, logOutput, "Failed to monitor nodes: error executing Lua script for monitoring nodes: Lua script error")

}

func TestStartNodeMonitoring_ImmediateStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	mockClient := createMockRedisClient()

	nodeList := "{node_tag}:active_nodes"

	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeList}, "monitor").Return(nil).Maybe()

	manager := &RedisManager{
		client:  mockClient,
		ctx:     ctx,
		LuaSHA:  "mockSHA",
		NodeTag: "node_tag",
	}

	var logBuffer bytes.Buffer
	logrus.SetOutput(&logBuffer)
	defer logrus.SetOutput(os.Stderr)

	manager.StartNodeMonitoring(1 * time.Second)

	cancel()

	time.Sleep(100 * time.Millisecond)

	mockClient.AssertExpectations(t)

	logOutput := logBuffer.String()
	assert.Contains(t, logOutput, "Node monitoring stopped")
}

func TestMonitorNodes_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	failedNodesResult := []interface{}{"node1", "node2"}
	nodeList := "{node_tag}:active_nodes"

	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeList}, "monitor").
		Return(redis.NewCmdResult(failedNodesResult, nil)).Once()

	manager := &RedisManager{
		client:  mockClient,
		ctx:     ctx,
		LuaSHA:  "mockSHA",
		NodeTag: "node_tag",
	}

	failedNodes, err := manager.monitorNodes()

	assert.NoError(t, err)
	assert.Equal(t, []string{"node1", "node2"}, failedNodes)

	mockClient.AssertExpectations(t)
}

func TestMonitorNodes_LuaScriptError(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	nodeList := "{node_tag}:active_nodes"

	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeList}, "monitor").
		Return(redis.NewCmdResult(nil, fmt.Errorf("Lua script error"))).Once()

	manager := &RedisManager{
		client:  mockClient,
		ctx:     ctx,
		LuaSHA:  "mockSHA",
		NodeTag: "node_tag",
	}

	failedNodes, err := manager.monitorNodes()

	assert.Error(t, err)
	assert.Nil(t, failedNodes)
	assert.Contains(t, err.Error(), "error executing Lua script for monitoring nodes: Lua script error")

	mockClient.AssertExpectations(t)
}

func TestMonitorNodes_InvalidNodeIDType(t *testing.T) {
	ctx := context.Background()
	mockClient := createMockRedisClient()

	invalidNodeIDResult := []interface{}{"node1", 12345}
	nodeList := "{node_tag}:active_nodes"

	mockClient.On("EvalSha", ctx, "mockSHA", []string{nodeList}, "monitor").
		Return(redis.NewCmdResult(invalidNodeIDResult, nil)).Once()

	manager := &RedisManager{
		client:  mockClient,
		ctx:     ctx,
		LuaSHA:  "mockSHA",
		NodeTag: "node_tag",
	}

	failedNodes, err := manager.monitorNodes()

	assert.Error(t, err)
	assert.Nil(t, failedNodes)
	assert.Contains(t, err.Error(), "unexpected node ID type: int")

	mockClient.AssertExpectations(t)
}
