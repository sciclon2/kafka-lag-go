package utils

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-redis/redis/v8"
)

func StoreOffsetsInRedis(pipe redis.Pipeliner, ctx context.Context, topicName string, offsets map[int32]*sarama.OffsetResponseBlock) {
	luaScript := `
	local key = KEYS[1]
	local score = tonumber(ARGV[1])
	local newTimestamp = ARGV[2]
	
	-- Fetch the last two members with their scores
	local members = redis.call('ZRANGE', key, -2, -1, 'WITHSCORES')
	local memberCount = #members / 2
	
	if memberCount == 2 then
		local secondLastScore = tonumber(members[2])
		local lastScore = tonumber(members[4])
	
		if secondLastScore == score and lastScore == score then
			local latestMember = members[3]
			redis.call('ZREM', key, latestMember)
			redis.call('ZADD', key, score, newTimestamp)
			return latestMember
		end
	end
	
	redis.call('ZADD', key, score, newTimestamp)
	return
	`

	for partition, block := range offsets {
		if block.Err != sarama.ErrNoError {
			log.Printf("Error in ListOffsets response for topic %s partition %d: %v", topicName, partition, block.Err)
			continue
		}
		offset := block.Offsets[0]

		fmt.Printf("Produced Offset - Topic: %s, Partition: %d, Offset: %d\n", topicName, partition, offset)

		timestamp := strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)
		key := fmt.Sprintf("%s:%d", topicName, partition)
		score := offset
		log.Printf("Storing offset in Redis - Topic: %s, Partition: %d, Offset: %d, Timestamp: %s", topicName, partition, offset, timestamp)

		// Execute the Lua script within the pipeline
		pipe.Eval(ctx, luaScript, []string{key}, score, timestamp)
	}
}
