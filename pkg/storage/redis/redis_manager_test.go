package redis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestNewRedisManager_Success(t *testing.T) {
	ctx := context.Background()

	mockClient := createMockRedisClient()
	mockLuaScript := "return redis.call('set', KEYS[1], ARGV[1])"
	mockClient.On("ScriptLoad", ctx, mockLuaScript).Return(redis.NewStringResult("mockSHA", nil))

	cfg := defaultRedisConfig()

	manager, err := NewRedisManager(ctx, mockClient, cfg, mockLuaScript)
	assert.NoError(t, err)
	assert.NotNil(t, manager)
	assert.Equal(t, "mockSHA", manager.LuaSHA)

	mockClient.AssertExpectations(t)
}

func TestNewRedisManager_FailScriptLoad(t *testing.T) {
	ctx := context.Background()

	mockClient := createMockRedisClient()
	mockLuaScript := "return redis.call('set', KEYS[1], ARGV[1])"
	mockClient.On("ScriptLoad", ctx, mockLuaScript).Return(redis.NewStringResult("", fmt.Errorf("failed to load script")))

	cfg := defaultRedisConfig()

	manager, err := NewRedisManager(ctx, mockClient, cfg, mockLuaScript)
	assert.Error(t, err)
	assert.Nil(t, manager)
	assert.Contains(t, err.Error(), "failed to load Lua script to Redis")

	mockClient.AssertExpectations(t)
}

func TestNewRedisManager_EmptySHA(t *testing.T) {
	ctx := context.Background()

	mockClient := createMockRedisClient()
	mockLuaScript := "return redis.call('set', KEYS[1], ARGV[1])"
	mockClient.On("ScriptLoad", ctx, mockLuaScript).Return(redis.NewStringResult("", nil))

	cfg := defaultRedisConfig()

	manager, err := NewRedisManager(ctx, mockClient, cfg, mockLuaScript)
	assert.NoError(t, err)
	assert.NotNil(t, manager)
	assert.Equal(t, "", manager.LuaSHA)

	mockClient.AssertExpectations(t)
}

func TestRedisManager_GracefulStop_Success(t *testing.T) {
	ctx := context.Background()
	cancelFunc := func() {
		// Simulate canceling the context
	}

	mockClient := createMockRedisClient()
	mockClient.On("Close").Return(nil)

	// Initialize the RedisManager with the mock client and a cancel function
	manager := &RedisManager{
		client:    mockClient,
		ctx:       ctx,
		cancelCtx: cancelFunc,
	}

	// Capture the log output and ensure it's reset after the test
	var logBuffer bytes.Buffer
	log.SetOutput(&logBuffer)
	t.Cleanup(func() {
		log.SetOutput(nil) // Restore original log output after test
	})
	// Call the GracefulStop method
	err := manager.GracefulStop()

	// Assert that no error occurred
	assert.NoError(t, err)

	// Verify that the mock's Close method was called
	mockClient.AssertExpectations(t)
}

func TestRedisManager_GracefulStop_CloseError(t *testing.T) {
	ctx := context.Background()
	cancelFunc := func() {
		// Simulate canceling the context
	}

	// Create a mock Redis client
	mockClient := createMockRedisClient()
	closeError := fmt.Errorf("close error")
	mockClient.On("Close").Return(closeError)

	// Initialize the RedisManager with the mock client and a cancel function
	manager := &RedisManager{
		client:    mockClient,
		ctx:       ctx,
		cancelCtx: cancelFunc,
	}

	// Call the GracefulStop method
	err := manager.GracefulStop()

	// Assert that an error occurred and that it wraps the expected error
	assert.Error(t, err)
	assert.True(t, errors.Is(err, closeError), "expected error to wrap close error")

	// Verify that the mock's Close method was called
	mockClient.AssertExpectations(t)
}
