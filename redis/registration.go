package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

// Lua script for atomic node registration and deregistration
const luaScript = `
-- Register Node
if KEYS[1] == "register" then
	local node_id = redis.call('INCR', 'node_id_counter')
	local node_key = 'node:' .. node_id
	local created = redis.call('SETNX', node_key, 'active')
	if created == 1 then
		redis.call('EXPIRE', node_key, 300) -- Set expiry to 5 minutes
		return node_id
	else
		return -1
	end
end

-- Deregister Node
if KEYS[1] == "deregister" then
	local node_key = 'node:' .. ARGV[1]
	local deleted = redis.call('DEL', node_key)
	return deleted
end
`

// RegisterNode registers a node with a unique ID using a Lua script
func RegisterNode(client *redis.Client) (int, error) {
	result, err := client.Eval(ctx, luaScript, []string{"register"}).Result()
	if err != nil {
		return 0, fmt.Errorf("error executing Lua script for registration: %v", err)
	}

	nodeID, ok := result.(int64)
	if !ok || nodeID == -1 {
		return 0, fmt.Errorf("failed to register node")
	}

	return int(nodeID), nil
}

// DeregisterNode deregisters a node by its ID using a Lua script
func DeregisterNode(client *redis.Client, nodeID int) error {
	_, err := client.Eval(ctx, luaScript, []string{"deregister"}, fmt.Sprintf("%d", nodeID)).Result()
	if err != nil {
		return fmt.Errorf("error executing Lua script for deregistration: %v", err)
	}
	return nil
}

// RefreshNode keeps the node registration alive
func RefreshNode(client *redis.Client, nodeID int) error {
	nodeKey := fmt.Sprintf("node:%d", nodeID)
	_, err := client.Expire(ctx, nodeKey, 5*time.Minute).Result()
	if err != nil {
		return fmt.Errorf("error refreshing node key: %v", err)
	}
	return nil
}
