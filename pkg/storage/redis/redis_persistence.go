package redis

import (
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/sciclon2/kafka-lag-go/pkg/structs"

	"github.com/redis/go-redis/v9"
)

// PersistLatestProducedOffsets reads from the groupStructCompleteChan, processes the data,
// and returns a channel with the processed data.
func (rm *RedisManager) PersistLatestProducedOffsets(groupsWithLeaderInfoAndLeaderOffsetsChan <-chan *structs.Group, numWorkers int) <-chan *structs.Group {
	groupsComplete := make(chan *structs.Group)

	var wg sync.WaitGroup

	// Start multiple goroutines to process the groups concurrently
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for group := range groupsWithLeaderInfoAndLeaderOffsetsChan {
				// Process the Group struct here (e.g., persist the latest produced offsets to storage)
				err := rm.addNewProducedOffsets(group)
				if err != nil {
					logrus.Errorf("Failed to persist data for group %s: %v", group.Name, err)
					continue
				}

				// Bring the offsets history to the Group struct
				rm.fetchProducedOffsetsHistory(group)

				// Send the processed Group struct pointer to the output channel
				groupsComplete <- group
			}
		}()
	}

	// Wait for all goroutines to finish processing
	go func() {
		wg.Wait()
		// Close the output channel after all input data is processed
		close(groupsComplete)
	}()

	return groupsComplete
}

//groupsWithLeaderInfoAndLeaderOffsetsChan

// processAndPersistGroup is a helper function that processes a group and persists its data to Redis.
func (rm *RedisManager) addNewProducedOffsets(group *structs.Group) error {
	pipe := rm.client.Pipeline()

	for _, topic := range group.Topics {
		for _, partition := range topic.Partitions {
			if partition.LatestProducedOffset == -1 || partition.LatestProducedOffsetAt == -1 {
				logrus.Debugf("Skipping partition %d in topic %s for group %s due to missing offset or timestamp", partition.Number, topic.Name, group.Name)
				continue
			}

			redisKey := fmt.Sprintf("%s:%s:%d", group.ClusterName, topic.Name, partition.Number)
			args := []interface{}{
				redisKey,                       // The Redis key for the ZSET
				partition.LatestProducedOffset, // The offset to add
				fmt.Sprintf("%d", partition.LatestProducedOffsetAt), // The timestamp for the new latest produced offset
				rm.TTLSeconds,
				20, // Cleanup probability (20%)
			}

			pipe.EvalSha(rm.ctx, rm.LuaSHA, []string{"add_latest_produced_offset"}, args...)
		}
	}

	cmders, err := pipe.Exec(rm.ctx)
	if err != nil {
		return fmt.Errorf("failed to execute Redis pipeline: %v", err)
	}

	for _, cmder := range cmders {
		if cmder.Err() != nil {
			logrus.Errorf("Error in pipeline command: %v", cmder.Err())
		}
	}
	return nil
}

// FetchProducedOffsetsHistory retrieves the produced offsets history for a group
// from Redis and populates the corresponding struct fields.
func (rm *RedisManager) fetchProducedOffsetsHistory(group *structs.Group) error {
	// Queue the commands and execute the pipeline
	pipeResults, err := rm.queueProducedOffsetsHistory(group)
	if err != nil {
		return err
	}
	// Process the results and populate the struct
	rm.processProducedOffsetsHistory(group, pipeResults)

	return nil
}

// queueProducedOffsetsHistory queues Redis commands to retrieve the produced offsets history
// for each partition in the group and returns the results.
func (rm *RedisManager) queueProducedOffsetsHistory(group *structs.Group) (map[string]*redis.ZSliceCmd, error) {
	// Create a Redis pipeline
	pipe := rm.client.Pipeline()

	// Map to hold the results from the pipeline
	pipeResults := make(map[string]*redis.ZSliceCmd)

	for _, topic := range group.Topics {
		for _, partition := range topic.Partitions {
			redisKey := fmt.Sprintf("%s:%s:%d", group.ClusterName, topic.Name, partition.Number)

			// Debugging: Log the redisKey being queued
			logrus.Debugf("Queuing ZRANGE WITHSCORES for Redis key: %s\n", redisKey)

			// Queue the ZRANGE WITHSCORES command in the pipeline and store the result command in the map
			pipeResults[redisKey] = pipe.ZRangeWithScores(rm.ctx, redisKey, 0, -1)
		}
	}

	// Execute the pipeline
	logrus.Debugf("Executing Redis pipeline for queued ZRANGE WITHSCORES commands.")
	_, err := pipe.Exec(rm.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute Redis pipeline: %v", err)
	}

	logrus.Debugf("Successfully executed Redis pipeline.")

	return pipeResults, nil
}

// processProducedOffsetsHistory processes the results of the Redis pipeline and populates
// the group's produced offsets history.
func (rm *RedisManager) processProducedOffsetsHistory(group *structs.Group, pipeResults map[string]*redis.ZSliceCmd) {
	for i := range group.Topics {
		topic := &group.Topics[i] // Get a pointer to the topic
		for j := range topic.Partitions {
			partition := &topic.Partitions[j] // Get a pointer to the partition

			redisKey := fmt.Sprintf("%s:%s:%d", group.ClusterName, topic.Name, partition.Number)
			logrus.Debugf("Processing Redis key: %s for Partition: %d\n", redisKey, partition.Number)

			// Retrieve the pipeline result for this key
			cmd := pipeResults[redisKey]
			if cmd == nil {
				// This shouldn't happen, but if it does, skip processing this partition
				logrus.Debugf("No result found in pipeline for key: %s\n", redisKey)
				continue
			}

			// Check for errors in the pipeline result
			producedOffsets, err := cmd.Result()
			logrus.Debugf("Cmd result for key %s: %+v, error: %v\n", redisKey, producedOffsets, err)
			if err != nil {
				logrus.Errorf("Error retrieving produced offsets for key: %s, skipping partition: %v. Error: %v\n", redisKey, partition.Number, err)
				continue
			}

			// Log the number of results fetched
			logrus.Debugf("Fetched %d produced offsets for Partition: %d (Key: %s)\n", len(producedOffsets), partition.Number, redisKey)

			// Handle flat lines by compressing them
			processedOffsets := compressFlatLines(producedOffsets)

			// If we have results, put them in the ProducedOffsetsHistory slice
			if len(processedOffsets) > 0 {
				partition.ProducedOffsetsHistory = processedOffsets
				logrus.Debugf("Stored %d produced offsets in ProducedOffsetsHistory for Partition: %d\n", len(processedOffsets), partition.Number)
			} else {
				logrus.Debugf("No produced offsets found for Partition: %d (Key: %s)\n", partition.Number, redisKey)
			}
		}
	}
}

func compressFlatLines(producedOffsets []redis.Z) []redis.Z {
	if len(producedOffsets) < 2 {
		logrus.Debugf("No compression needed, returning original data")
		return producedOffsets
	}

	compressed := make([]redis.Z, 0, len(producedOffsets))
	lastOffset := producedOffsets[0].Score
	latestTimestamp := producedOffsets[0].Member.(string)

	for i := 1; i < len(producedOffsets); i++ {
		currentOffset := producedOffsets[i].Score
		currentTimestamp := producedOffsets[i].Member.(string)

		if currentOffset == lastOffset {
			// Update the latest timestamp if the offset is the same as the last one
			latestTimestamp = currentTimestamp
		} else {
			// Add the entry for the last offset with the latest timestamp
			compressed = append(compressed, redis.Z{Score: lastOffset, Member: latestTimestamp})

			// Reset for the next group of offsets
			lastOffset = currentOffset
			latestTimestamp = currentTimestamp
		}
	}

	// Add the final entry with the latest timestamp
	compressed = append(compressed, redis.Z{Score: lastOffset, Member: latestTimestamp})

	logrus.Debugf("Final compressed data: %+v", compressed)
	return compressed
}
