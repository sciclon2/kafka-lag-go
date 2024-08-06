package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// CreateRedisClient creates a new Redis client using the provided address
func CreateRedisClient(redisAddress string) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "", // No password set
		DB:       0,  // Use default DB
	})
	return client
}

var ctx = context.Background()

// Lua script for atomic node registration, deregistration, and monitoring
const luaScript = `
-- Register Node
if KEYS[1] == "register" then
	local node_key = 'node:' .. ARGV[1]
	local ttl = ARGV[2]
	local created = redis.call('SETNX', node_key, 'active')
	if created == 1 then
		redis.call('EXPIRE', node_key, ttl)
		redis.call('SADD', 'active_nodes', ARGV[1])
		return node_key
	else
		return -1
	end
end

-- Deregister Node
if KEYS[1] == "deregister" then
	local node_key = 'node:' .. ARGV[1]
	local deleted = redis.call('DEL', node_key)
	if deleted == 1 then
		redis.call('SREM', 'active_nodes', ARGV[1])
	end
	return deleted
end

-- Monitor Nodes and Remove Failed Nodes
if KEYS[1] == "monitor" then
	local failed_nodes = {}
	local node_ids = redis.call('SMEMBERS', 'active_nodes')
	for _, node_id in ipairs(node_ids) do
		local node_key = 'node:' .. node_id
		local ttl = redis.call('TTL', node_key)
		if ttl < 0 then
			table.insert(failed_nodes, node_id)
			redis.call('DEL', node_key)
			redis.call('SREM', 'active_nodes', node_id)
		end
	end
	return failed_nodes
end
`

// RegisterNode registers a node with a unique ID using a Lua script
func RegisterNode(client *redis.Client, nodeID string, ttl int) error {
	_, err := client.Eval(ctx, luaScript, []string{"register"}, nodeID, ttl).Result()
	if err != nil {
		return fmt.Errorf("error executing Lua script for registration: %v", err)
	}
	return nil
}

// DeregisterNode deregisters a node by its ID using a Lua script
func DeregisterNode(client *redis.Client, nodeID string) error {
	_, err := client.Eval(ctx, luaScript, []string{"deregister"}, nodeID).Result()
	if err != nil {
		return fmt.Errorf("error executing Lua script for deregistration: %v", err)
	}
	return nil
}

// RefreshNode keeps the node registration alive
func RefreshNode(client *redis.Client, nodeID string, ttl int) error {
	nodeKey := fmt.Sprintf("node:%s", nodeID)
	_, err := client.Expire(ctx, nodeKey, time.Duration(ttl)*time.Second).Result()
	if err != nil {
		return fmt.Errorf("error refreshing node key: %v", err)
	}
	return nil
}

// MonitorNodes checks the status of all nodes and removes failed ones
func MonitorNodes(client *redis.Client) ([]string, error) {
	result, err := client.Eval(ctx, luaScript, []string{"monitor"}).Result()
	if err != nil {
		return nil, fmt.Errorf("error executing Lua script for monitoring: %v", err)
	}

	failedNodes, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected result type: %T", result)
	}

	failedNodeIDs := make([]string, len(failedNodes))
	for i, nodeID := range failedNodes {
		failedNodeIDs[i] = nodeID.(string)
	}

	return failedNodeIDs, nil
}

// GetActiveNodes retrieves the list of active nodes
func GetActiveNodes(client *redis.Client) ([]string, error) {
	nodeIDs, err := client.SMembers(ctx, "active_nodes").Result()
	if err != nil {
		return nil, fmt.Errorf("error getting active nodes: %v", err)
	}
	return nodeIDs, nil
}

// GetNodeIndex returns the index of the given nodeID in the active nodes list
func GetNodeIndex(nodeID string, activeNodes []string) (int, error) {
	for index, id := range activeNodes {
		if id == nodeID {
			return index, nil
		}
	}
	return -1, fmt.Errorf("node ID %s not found in active nodes", nodeID)
}
