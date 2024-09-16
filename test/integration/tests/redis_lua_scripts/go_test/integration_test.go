package integration

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	redis_local "github.com/sciclon2/kafka-lag-go/pkg/storage/redis"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "true" {
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// Initialization function to set up the test environment
func initRedisTest(ctx context.Context, luaScript, nodeTag string) (*redis.Client, string, error) {
	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // Adjust Redis server address if needed
	})

	// Load the Lua script into Redis and get the SHA1 hash
	sha, err := rdb.ScriptLoad(ctx, luaScript).Result()
	if err != nil {
		return nil, "", err
	}

	// Clean any existing test data
	rdb.Del(ctx, nodeTag+":node-1", nodeTag+":node-2", nodeTag+":node-3", nodeTag+":active_nodes")

	return rdb, sha, nil
}

// Cleanup function to remove test data after test execution
func cleanupRedisTest(ctx context.Context, rdb *redis.Client, nodeTag string) {
	rdb.Del(ctx, nodeTag+":node-1", nodeTag+":node-2", nodeTag+":active_nodes")
}

// Helper function to deregister a node
func deregisterNode(ctx context.Context, rdb *redis.Client, sha, nodeTag, nodeKey, nodeList string) (int64, error) {
	// Prepare the KEYS and ARGV for deregistration
	keys := []string{nodeKey}
	args := []interface{}{"deregister", nodeList}

	// Execute the Lua script for deregistration
	deleted, err := rdb.EvalSha(ctx, sha, keys, args...).Result()
	if err != nil {
		return 0, fmt.Errorf("Failed to execute Lua script for deregistering %s: %v", nodeKey, err)
	}

	return deleted.(int64), nil
}

// Helper function to register or refresh a node without making assertions
func registerOrRefreshNode(ctx context.Context, rdb *redis.Client, sha, nodeTag, nodeKey, nodeList string, ttlSeconds ...int) (int64, int64, time.Duration, error) {
	ttl := 60
	if len(ttlSeconds) > 0 {
		ttl = ttlSeconds[0]
	}
	// Prepare the KEYS and ARGV
	keys := []string{nodeKey}
	args := []interface{}{"register_or_refresh", nodeList, ttl}

	// Execute the Lua script
	_, err := rdb.EvalSha(ctx, sha, keys, args...).Result()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("Failed to execute Lua script for %s: %v", nodeKey, err)
	}

	// Get the node index in the active_nodes list
	nodeIndex, err := rdb.LPos(ctx, nodeList, nodeKey, redis.LPosArgs{}).Result()
	if err != nil {
		return 0, 0, 0, err
	}

	// Check if the node key exists
	exists, err := rdb.Exists(ctx, nodeKey).Result()
	if err != nil {
		return 0, 0, 0, err
	}

	// Get the TTL for the node
	ttlVal, err := rdb.TTL(ctx, nodeKey).Result()
	if err != nil {
		return 0, 0, 0, err
	}

	return nodeIndex, exists, ttlVal, nil
}

// The main test function for registering or refreshing a single node
func TestRegisterOrRefreshNode(t *testing.T) {

	ctx := context.Background()
	nodeTag := "nodetag"

	// Initialize the Redis test environment
	rdb, sha, err := initRedisTest(ctx, redis_local.LuaScriptContent, nodeTag)
	if err != nil {
		log.Fatalf("Failed to initialize Redis test: %v", err)
	}
	defer cleanupRedisTest(ctx, rdb, nodeTag)

	// Register and validate the node
	nodeKey := nodeTag + ":node-1"
	nodeList := nodeTag + ":active_nodes"

	nodeIndex, exists, ttl, err := registerOrRefreshNode(ctx, rdb, sha, nodeTag, nodeKey, nodeList)
	assert.NoError(t, err, "Error during node registration")

	// Perform assertions in the test
	assert.Equal(t, int64(0), nodeIndex, "Node-1 should be at index 0 in active_nodes list")
	assert.Equal(t, int64(1), exists, "Node-1 key should exist in Redis")
	assert.True(t, ttl > 0 && ttl <= 60*time.Second, "TTL for Node-1 should be set to a value within the range of 0 to 60 seconds")

}

// The main test function for registering or refreshing two nodes
func TestRegisterOrRefreshTwoNodes(t *testing.T) {
	ctx := context.Background()
	nodeTag := "nodetag" // Replace with your actual nodetag value

	// Initialize the Redis test environment
	rdb, sha, err := initRedisTest(ctx, redis_local.LuaScriptContent, nodeTag)
	if err != nil {
		log.Fatalf("Failed to initialize Redis test: %v", err)
	}
	defer cleanupRedisTest(ctx, rdb, nodeTag)

	// Register and validate the first node
	nodeKey1 := nodeTag + ":node-1"
	nodeList := nodeTag + ":active_nodes"

	nodeIndex1, exists1, ttl1, err := registerOrRefreshNode(ctx, rdb, sha, nodeTag, nodeKey1, nodeList)
	assert.NoError(t, err, "Error during first node registration")

	// Perform assertions for Node-1
	assert.Equal(t, int64(0), nodeIndex1, "Node-1 should be at index 0 in active_nodes list")
	assert.Equal(t, int64(1), exists1, "Node-1 key should exist in Redis")
	assert.True(t, ttl1 > 0 && ttl1 <= 60*time.Second, "TTL for Node-1 should be set to a value within the range of 0 to 60 seconds")

	// Register and validate the second node
	nodeKey2 := nodeTag + ":node-2"

	nodeIndex2, exists2, ttl2, err := registerOrRefreshNode(ctx, rdb, sha, nodeTag, nodeKey2, nodeList)
	assert.NoError(t, err, "Error during second node registration")

	// Perform assertions for Node-2
	assert.Equal(t, int64(1), nodeIndex2, "Node-2 should be at index 1 in active_nodes list")
	assert.Equal(t, int64(1), exists2, "Node-2 key should exist in Redis")
	assert.True(t, ttl2 > 0 && ttl2 <= 60*time.Second, "TTL for Node-2 should be set to a value within the range of 0 to 60 seconds")
}

// Test function for adding nodes in a loop, deregistering the middle one, and checking the list
func TestDeregisterNode(t *testing.T) {
	ctx := context.Background()
	nodeTag := "nodetag" // Replace with your actual nodetag value

	// Initialize the Redis test environment
	rdb, sha, err := initRedisTest(ctx, redis_local.LuaScriptContent, nodeTag)
	if err != nil {
		log.Fatalf("Failed to initialize Redis test: %v", err)
	}
	defer cleanupRedisTest(ctx, rdb, nodeTag)

	nodeList := nodeTag + ":active_nodes"

	// Register and validate multiple nodes using a loop
	totalNodes := 3
	for i := 1; i <= totalNodes; i++ {
		nodeKey := fmt.Sprintf("%s:node-%d", nodeTag, i)
		_, _, _, err := registerOrRefreshNode(ctx, rdb, sha, nodeTag, nodeKey, nodeList)
		assert.NoError(t, err, fmt.Sprintf("Error during node-%d registration", i))
	}

	// Validate the initial positions of the nodes in the list
	for i := 1; i <= totalNodes; i++ {
		nodeKey := fmt.Sprintf("%s:node-%d", nodeTag, i)
		nodeIndex, err := rdb.LPos(ctx, nodeList, nodeKey, redis.LPosArgs{}).Result()
		assert.NoError(t, err, fmt.Sprintf("Error finding node-%d in active_nodes list", i))
		assert.Equal(t, int64(i-1), nodeIndex, fmt.Sprintf("Node-%d should be at index %d in active_nodes list", i, i-1))
	}

	// Deregister the second node (node-2)
	nodeKey2 := fmt.Sprintf("%s:node-2", nodeTag)
	deleted, err := deregisterNode(ctx, rdb, sha, nodeTag, nodeKey2, nodeList)
	assert.NoError(t, err, "Error during node-2 deregistration")
	assert.Equal(t, int64(1), deleted, "Node-2 should have been deleted")

	// Validate the positions of the remaining nodes after deregistration
	nodeKey1 := fmt.Sprintf("%s:node-1", nodeTag)
	nodeKey3 := fmt.Sprintf("%s:node-3", nodeTag)
	listLen, err := rdb.LLen(ctx, nodeList).Result()

	// Ensure the length of the list is correct
	assert.NoError(t, err, "Error getting the length of active_nodes list")
	assert.Equal(t, int64(2), listLen, "The length of active_nodes should be 2 after deregistering node-2")

	// Node-1 should still be at index 0
	nodeIndex1, err := rdb.LPos(ctx, nodeList, nodeKey1, redis.LPosArgs{}).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), nodeIndex1, "Node-1 should still be at index 0 in active_nodes list")

	// Node-3 should now be at index 1
	nodeIndex3, err := rdb.LPos(ctx, nodeList, nodeKey3, redis.LPosArgs{}).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), nodeIndex3, "Node-3 should now be at index 1 in active_nodes list")

	// Ensure node-2 no longer exists in the list
	_, err = rdb.LPos(ctx, nodeList, nodeKey2, redis.LPosArgs{}).Result()
	assert.Error(t, err, "Node-2 should no longer exist in the active_nodes list")

	// Check that node-1 and node-3 still exist, but node-2 does not
	existsNode1, err := rdb.Exists(ctx, nodeKey1).Result()
	assert.NoError(t, err, "Error checking existence of node-1")
	assert.Equal(t, int64(1), existsNode1, "Node-1 should exist in Redis")

	existsNode3, err := rdb.Exists(ctx, nodeKey3).Result()
	assert.NoError(t, err, "Error checking existence of node-3")
	assert.Equal(t, int64(1), existsNode3, "Node-3 should exist in Redis")

	existsNode2, err := rdb.Exists(ctx, nodeKey2).Result()
	assert.NoError(t, err, "Error checking existence of node-2")
	assert.Equal(t, int64(0), existsNode2, "Node-2 should not exist in Redis")
}

// Test function to create nodes, get node info, and validate the response
func TestGetNodeInfo(t *testing.T) {
	ctx := context.Background()
	nodeTag := "nodetag" // Replace with your actual nodetag value

	// Initialize the Redis test environment
	rdb, sha, err := initRedisTest(ctx, redis_local.LuaScriptContent, nodeTag)
	assert.NoError(t, err)
	defer cleanupRedisTest(ctx, rdb, nodeTag)

	nodeList := nodeTag + ":active_nodes"

	// Register two nodes
	totalNodes := 2
	for i := 1; i <= totalNodes; i++ {
		nodeKey := fmt.Sprintf("%s:node-%d", nodeTag, i)
		_, _, _, err := registerOrRefreshNode(ctx, rdb, sha, nodeTag, nodeKey, nodeList)
		assert.NoError(t, err, fmt.Sprintf("Error during node-%d registration", i))
	}

	// Call the Lua script to get node info for node-1
	nodeKey1 := fmt.Sprintf("%s:node-1", nodeTag)
	keys := []string{nodeKey1}
	args := []interface{}{"get_node_info", nodeList}

	// Execute the Lua script to get node info
	result, err := rdb.EvalSha(ctx, sha, keys, args...).Result()
	assert.NoError(t, err, "Error executing Lua script for get_node_info")

	// Parse and validate the result
	res := result.([]interface{})
	assert.Equal(t, "ok", res[0], "The response status should be 'ok'")
	assert.Equal(t, int64(0), res[1].(int64), "Node-1 should be at index 0 in active_nodes list")
	assert.Equal(t, int64(2), res[2].(int64), "Total nodes should be 2")

	// Call the Lua script to get node info for node-2
	nodeKey2 := fmt.Sprintf("%s:node-2", nodeTag)
	keys = []string{nodeKey2}
	result, err = rdb.EvalSha(ctx, sha, keys, args...).Result()
	assert.NoError(t, err, "Error executing Lua script for get_node_info")

	// Parse and validate the result for node-2
	res = result.([]interface{})
	assert.Equal(t, "ok", res[0], "The response status should be 'ok'")
	assert.Equal(t, int64(1), res[1].(int64), "Node-2 should be at index 1 in active_nodes list")
	assert.Equal(t, int64(2), res[2].(int64), "Total nodes should still be 2")

	// Validate response for a non-existing node
	nodeKey3 := fmt.Sprintf("%s:node-3", nodeTag)
	keys = []string{nodeKey3}
	result, err = rdb.EvalSha(ctx, sha, keys, args...).Result()
	assert.NoError(t, err, "Error executing Lua script for get_node_info with non-existing node")

	// Parse and validate the result for a non-existing node
	res = result.([]interface{})
	assert.Equal(t, "not_found", res[0], "The response status should be 'not_found'")
	assert.Equal(t, int64(-1), res[1].(int64), "The index should be -1 for a non-existing node")
	assert.Equal(t, int64(-1), res[2].(int64), "Total nodes should be -1 for a non-existing node")
}

// Test function to monitor and remove failed nodes and ensure the correct node remains
func TestMonitorAndRemoveFailedNodes(t *testing.T) {
	ctx := context.Background()
	nodeTag := "nodetag" // Replace with your actual nodetag value

	// Initialize the Redis test environment
	rdb, sha, err := initRedisTest(ctx, redis_local.LuaScriptContent, nodeTag)
	assert.NoError(t, err)
	defer cleanupRedisTest(ctx, rdb, nodeTag)

	nodeList := nodeTag + ":active_nodes"

	// Register two nodes: node-1 with a default TTL (60 seconds) and node-2 with a TTL of 1 second
	nodeKey1 := fmt.Sprintf("%s:node-1", nodeTag)
	nodeKey2 := fmt.Sprintf("%s:node-2", nodeTag)
	_, _, _, err = registerOrRefreshNode(ctx, rdb, sha, nodeTag, nodeKey1, nodeList)
	assert.NoError(t, err, "Error during node-1 registration")
	_, _, _, err = registerOrRefreshNode(ctx, rdb, sha, nodeTag, nodeKey2, nodeList, 1)
	assert.NoError(t, err, "Error during node-2 registration")

	// Sleep for 2 seconds to allow node-2 to expire
	time.Sleep(2 * time.Second)

	// Execute the "monitor" script to detect and remove failed nodes
	keys := []string{nodeList}
	args := []interface{}{"monitor"}
	result, err := rdb.EvalSha(ctx, sha, keys, args...).Result()
	assert.NoError(t, err, "Error executing Lua script for monitoring nodes")

	// Validate the failed nodes
	failedNodes := result.([]interface{})
	assert.Contains(t, failedNodes, nodeKey2, "Node-2 should be marked as a failed node")

	// Ensure node-2 is removed from the active_nodes list
	listLen, err := rdb.LLen(ctx, nodeList).Result()
	assert.NoError(t, err, "Error getting the length of active_nodes list")
	assert.Equal(t, int64(1), listLen, "The length of active_nodes should be 1 after removing node-2")

	// Ensure node-1 still exists
	existsNode1, err := rdb.Exists(ctx, nodeKey1).Result()
	assert.NoError(t, err, "Error checking existence of node-1")
	assert.Equal(t, int64(1), existsNode1, "Node-1 should still exist in Redis")

	// Ensure node-2 no longer exists
	existsNode2, err := rdb.Exists(ctx, nodeKey2).Result()
	assert.NoError(t, err, "Error checking existence of node-2")
	assert.Equal(t, int64(0), existsNode2, "Node-2 should no longer exist in Redis")

	// Additional check: Ensure the correct entry remains in active_nodes (node-1)
	activeNodes, err := rdb.LRange(ctx, nodeList, 0, -1).Result()
	assert.NoError(t, err, "Error fetching active_nodes list")
	assert.Equal(t, []string{nodeKey1}, activeNodes, "The only entry in active_nodes should be node-1")
}

// Test function for adding multiple offsets and verifying order in the sorted set
func TestAddLatestProducedOffset_MultipleEntries(t *testing.T) {
	ctx := context.Background()
	nodeTag := "test_key" // This key will be used for the test

	// Initialize the Redis test environment
	rdb, sha, err := initRedisTest(ctx, redis_local.LuaScriptContent, nodeTag)
	assert.NoError(t, err, "Failed to initialize Redis test environment")
	defer cleanupRedisTest(ctx, rdb, nodeTag)

	// Define multiple offsets and timestamps
	entries := []struct {
		offset    int
		timestamp int64
	}{
		{100, time.Now().Unix()},
		{200, time.Now().Unix() + 1},
		{300, time.Now().Unix() + 2},
	}
	ttlSeconds := 60

	// Add multiple entries using the Lua script
	for _, entry := range entries {
		keys := []string{nodeTag}
		args := []interface{}{"add_latest_produced_offset", entry.offset, entry.timestamp, ttlSeconds, 0}

		// Run the Lua script to add the offset and timestamp
		res, err := rdb.EvalSha(ctx, sha, keys, args...).Result()
		assert.NoError(t, err, "Error running Lua script")
		assert.Equal(t, fmt.Sprintf("Added or replaced member with timestamp %d", entry.timestamp), res, "Unexpected result from Lua script")
	}

	// Verify that all entries exist in the sorted set and in the correct order
	members, err := rdb.ZRangeWithScores(ctx, nodeTag, 0, -1).Result()
	assert.NoError(t, err, "Error fetching members from the sorted set")
	assert.Len(t, members, 3, "There should be 3 members in the sorted set")

	// Verify the order of the entries by checking the offsets and timestamps
	for i, entry := range entries {
		assert.Equal(t, float64(entry.offset), members[i].Score, "The offset value should match")
		assert.Equal(t, fmt.Sprintf("%d", entry.timestamp), members[i].Member.(string), "The timestamp should match")
	}

	// Check that the TTL is correctly set for the key
	ttl, err := rdb.TTL(ctx, nodeTag).Result()
	assert.NoError(t, err, "Error checking TTL of the key")
	assert.GreaterOrEqual(t, ttl.Seconds(), float64(ttlSeconds-1), "TTL should be close to 60 seconds")
}

// Test function for TTL expiration
func TestAddLatestProducedOffset_TTLExpiration(t *testing.T) {
	ctx := context.Background()
	nodeTag := "test_key_ttl_expiration" // This key will be used for the test

	// Initialize the Redis test environment
	rdb, sha, err := initRedisTest(ctx, redis_local.LuaScriptContent, nodeTag)
	assert.NoError(t, err, "Failed to initialize Redis test environment")
	defer cleanupRedisTest(ctx, rdb, nodeTag)

	// Define offset, timestamp, and short TTL (1 second)
	offset := 100
	timestamp := time.Now().Unix()
	ttlSeconds := 1

	// Add the entry using the Lua script
	keys := []string{nodeTag}
	args := []interface{}{"add_latest_produced_offset", offset, timestamp, ttlSeconds, 0}

	// Run the Lua script to add the offset and timestamp
	res, err := rdb.EvalSha(ctx, sha, keys, args...).Result()
	assert.NoError(t, err, "Error running Lua script")
	assert.Equal(t, fmt.Sprintf("Added or replaced member with timestamp %d", timestamp), res, "Unexpected result from Lua script")

	// Verify that the entry exists immediately after insertion
	members, err := rdb.ZRangeWithScores(ctx, nodeTag, 0, -1).Result()
	assert.NoError(t, err, "Error fetching members from the sorted set")
	assert.Len(t, members, 1, "There should be 1 member in the sorted set")

	// Wait for 2 seconds to ensure the TTL has expired
	time.Sleep(2 * time.Second)

	// Verify that the key is no longer present in Redis
	exists, err := rdb.Exists(ctx, nodeTag).Result()
	assert.NoError(t, err, "Error checking if key exists after TTL expiration")
	assert.Equal(t, int64(0), exists, "The key should no longer exist after TTL expiration")
}

// Test function for cleanup logic with probability
func TestAddLatestProducedOffset_CleanupLogic(t *testing.T) {
	ctx := context.Background()
	nodeTag := "test_key_cleanup" // This key will be used for the test

	// Initialize the Redis test environment
	rdb, sha, err := initRedisTest(ctx, redis_local.LuaScriptContent, nodeTag)
	assert.NoError(t, err, "Failed to initialize Redis test environment")
	defer cleanupRedisTest(ctx, rdb, nodeTag)

	// Define initial entries with TTL (to simulate old data)
	entries := []struct {
		offset    int
		timestamp int64
	}{
		{100, time.Now().UnixMilli() - 120000}, // 2 minutes old (in milliseconds)
		{200, time.Now().UnixMilli() - 60000},  // 1 minute old (in milliseconds)
	}
	ttlSeconds := 600 // Set TTL to 10 minutes for the key epxiration

	// Add the old entries
	for _, entry := range entries {
		keys := []string{nodeTag}
		args := []interface{}{"add_latest_produced_offset", entry.offset, entry.timestamp, ttlSeconds, 0}
		_, err := rdb.EvalSha(ctx, sha, keys, args...).Result()
		assert.NoError(t, err, "Error adding initial entry")
	}

	// Verify that the initial entries exist in the sorted set
	members, err := rdb.ZRangeWithScores(ctx, nodeTag, 0, -1).Result()
	assert.NoError(t, err, "Error fetching members from the sorted set")
	assert.Len(t, members, 2, "There should be 2 initial members in the sorted set")

	// Add a new entry and trigger cleanup with 100% probability
	newOffset := 300
	newTimestamp := time.Now().UnixMilli()
	cleanupProbability := 100 // Guarantee cleanup
	ttlSeconds = 30           // Set TTL to 30 seconds, this will add to the key expiration 30 more seconds but the most important is it will delete entries older than 30 seconds
	args := []interface{}{"add_latest_produced_offset", newOffset, newTimestamp, ttlSeconds, cleanupProbability}

	// Run the Lua script to add the new offset and trigger cleanup
	_, err = rdb.EvalSha(ctx, sha, []string{nodeTag}, args...).Result()
	assert.NoError(t, err, "Error running Lua script with 100% cleanup probability")

	// Verify that old entries are removed after cleanup
	members, err = rdb.ZRangeWithScores(ctx, nodeTag, 0, -1).Result()
	assert.NoError(t, err, "Error fetching members after cleanup")
	assert.Len(t, members, 1, "Only 1 entry should remain after cleanup (new entry)")
	assert.Equal(t, float64(newOffset), members[0].Score, "The remaining entry should be the new offset")

	// Assert that the remaining entry after cleanup is the new entry (300 offset)
	assert.Equal(t, float64(newOffset), members[0].Score, "The remaining entry should be the new offset 300")
	assert.Equal(t, fmt.Sprintf("%d", newTimestamp), members[0].Member.(string), "The remaining timestamp should match the new entry's timestamp")

	// Now, test with 0% cleanup probability (no cleanup)
	// Add another entry but ensure no cleanup happens
	noCleanupOffset := 400
	noCleanupTimestamp := time.Now().UnixMilli()
	noCleanupProbability := 0
	args = []interface{}{"add_latest_produced_offset", noCleanupOffset, noCleanupTimestamp, ttlSeconds, noCleanupProbability}

	// Run the Lua script with 0% cleanup probability
	_, err = rdb.EvalSha(ctx, sha, []string{nodeTag}, args...).Result()
	assert.NoError(t, err, "Error running Lua script with 0% cleanup probability")

	// Verify that no cleanup occurred and both entries exist
	members, err = rdb.ZRangeWithScores(ctx, nodeTag, 0, -1).Result()
	assert.NoError(t, err, "Error fetching members after no cleanup")
	assert.Len(t, members, 2, "Both entries should remain when cleanup probability is 0%")

	// Verify the first entry is the one with offset 300
	assert.Equal(t, float64(newOffset), members[0].Score, "The first entry should be the new offset 300")
	assert.Equal(t, fmt.Sprintf("%d", newTimestamp), members[0].Member.(string), "The first entry's timestamp should match the new entry's timestamp")

	// Verify the second entry is the one with offset 400
	assert.Equal(t, float64(noCleanupOffset), members[1].Score, "The second entry should be the new offset 400")
	assert.Equal(t, fmt.Sprintf("%d", noCleanupTimestamp), members[1].Member.(string), "The second entry's timestamp should match the new entry's timestamp")
}
