package redis

// Your Lua script as a string
var LuaScriptContent = `
-- Deregister Node Script
if ARGV[1] == "deregister" then
    local node_key = KEYS[1]  -- The node key with the hash tag
    local node_list = ARGV[2] -- Node list passed as a parameter

    -- Attempt to delete the node key
    local deleted = redis.call('DEL', node_key)
    if deleted == 1 then
        -- Remove the node key from the active_nodes list using the tag
        redis.call('LREM', node_list, 0, node_key)
    end
    return deleted
end

if ARGV[1] == "register_or_refresh" then
    local node_key = KEYS[1]  -- Unique node key passed as the first key
    local node_list = ARGV[2] -- Node list passed as a parameter
    local ttl = tonumber(ARGV[3])  -- TTL value

    -- Attempt to set the node key if it doesn't exist
    local created = redis.call('SETNX', node_key, 'active')
    if created == 1 then
        -- If the key was created, set its TTL
        redis.call('EXPIRE', node_key, ttl)
    else
        -- If the key already exists, just refresh its TTL
        redis.call('EXPIRE', node_key, ttl)
    end

    -- Ensure the node key is in the node_list
    if redis.call("LPOS", node_list, node_key) == false then
        redis.call('RPUSH', node_list, node_key)
    end

    -- Retrieve and return the index of the node in the node_list
    local node_index = redis.call('LPOS', node_list, node_key)
    return node_index
end

-- Get Node Index and Total Nodes Script
if ARGV[1] == "get_node_info" then
    local node_key = KEYS[1]   -- The node key with the hash tag
    local node_list = ARGV[2]  -- The node list passed as a parameter

    -- Retrieve the list of active nodes
    local node_index_list = redis.call('LRANGE', node_list, 0, -1)
    if not node_index_list then
        return {'error', 'failed to retrieve active nodes list'}
    end

    local index = nil
    for i, v in ipairs(node_index_list) do
        if v == node_key then
            index = i - 1 -- Zero-based index
            break
        end
    end

    if index == nil then
        return {'not_found', -1, -1}
    end

    local total_nodes = redis.call('LLEN', node_list)
    if not total_nodes then
        return {'error', 'failed to retrieve total nodes count'}
    end

    return {'ok', index, total_nodes}
end

-- Monitor Nodes and Remove Failed Nodes
if ARGV[1] == "monitor" then
    local node_list = KEYS[1]  -- Node list passed
    local failed_nodes = {}

    -- Retrieve all node IDs from the active_nodes list
    local node_ids = redis.call('LRANGE', node_list, 0, -1)

    for i, node_id in ipairs(node_ids) do
        local ttl = redis.call('TTL', node_id)  -- No need to construct node_key

        -- If TTL is negative, the key has expired or does not exist
        if ttl < 0 then
            -- Add the node_id to the failed_nodes list
            table.insert(failed_nodes, node_id)

            -- Delete the node key and remove the node_id from active_nodes
            redis.call('DEL', node_id)  -- Delete the node key
            redis.call('LREM', node_list, 0, node_id)  -- Remove from active_nodes list
        end
    end

    -- Return the list of failed nodes
    return failed_nodes
end

if ARGV[1] == "add_latest_produced_offset" then
    local log = ""  -- Initialize an empty log string

    local key = KEYS[1]  -- Use the key from KEYS[1]
    local offset = tonumber(ARGV[2])
    local newTimestamp = tonumber(ARGV[3])
    local ttlSeconds = tonumber(ARGV[4])  -- TTL value in seconds
    local cleanupProbability = tonumber(ARGV[5])  -- Probability as a percentage (e.g., 20 for 20%)

    log = log .. "Processing key: " .. key .. ", Offset: " .. offset .. ", Timestamp: " .. newTimestamp .. "\n"
    log = log .. "TTL: " .. ttlSeconds .. "s, Cleanup Probability: " .. cleanupProbability .. "%\n"

    -- Log the current contents of the sorted set
    local currentMembers = redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
    log = log .. "Current members in the sorted set:\n"
    for i = 1, #currentMembers, 2 do
        local member = currentMembers[i]
        local score = currentMembers[i + 1]
        log = log .. "  Member: " .. member .. ", Score: " .. score .. "\n"
    end

    -- Optionally perform cleanup
    if math.random(100) < cleanupProbability then
        local expiredTimestamp = newTimestamp - (ttlSeconds * 1000)  -- TTL converted to milliseconds
        log = log .. "Cleanup triggered. Expired timestamp threshold: " .. expiredTimestamp .. "\n"

        -- Find and remove old entries by member (timestamp)
        local oldMembers = redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
        log = log .. "Found " .. (#oldMembers / 2) .. " members in the sorted set.\n"
        
        for i = 1, #oldMembers, 2 do
            local member = oldMembers[i]
            local timestamp = tonumber(member)
            log = log .. "Checking member with timestamp: " .. timestamp .. "\n"
            if timestamp < expiredTimestamp then
                redis.call('ZREM', key, member)
                log = log .. "Removed member with timestamp: " .. timestamp .. "\n"
            end
        end
    else
        log = log .. "Cleanup not triggered.\n"
    end

    -- Retrieve the last two entries in the ZSET
    local members = redis.call('ZRANGE', key, -2, -1, 'WITHSCORES')
    local memberCount = #members / 2
    log = log .. "There are " .. memberCount .. " members in the last two entries.\n"

    if memberCount == 2 then
        local secondLastOffset = tonumber(members[2])
        local lastOffset = tonumber(members[4])
        log = log .. "Second last offset: " .. secondLastOffset .. ", Last offset: " .. lastOffset .. "\n"

        if secondLastOffset == offset and lastOffset == offset then
            local latestMember = members[3]
            redis.call('ZREM', key, latestMember)
            log = log .. "Removed latest member with timestamp: " .. latestMember .. " due to duplicate offsets.\n"
        end
    end

    -- Add the new member (whether it's a replacement or a new entry)
    redis.call('ZADD', key, offset, newTimestamp)
    redis.call('EXPIRE', key, ttlSeconds)  -- Renew TTL
    log = log .. "Added/Updated member with timestamp: " .. newTimestamp .. " and offset: " .. offset .. "\n"
    log = log .. "TTL set to " .. ttlSeconds .. " seconds for key: " .. key .. "\n"

    -- Log the contents of the sorted set after modification
    local updatedMembers = redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
    log = log .. "Updated members in the sorted set:\n"
    for i = 1, #updatedMembers, 2 do
        local member = updatedMembers[i]
        local score = updatedMembers[i + 1]
        log = log .. "  Member: " .. member .. ", Score: " .. score .. "\n"
    end

    return log
end
`
