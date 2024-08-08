package metrics

import (
	"context"
	"fmt"
	"kafka-lag/structs"
	"log"
	"sort"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
)

var ctx = context.Background()

func CalculateLag(groupName string, producedOffsets, committedOffsets map[string]map[int32]int64, redisClient *redis.Client) {
	currentTime := time.Now().UnixNano() / int64(time.Millisecond)

	for topic, partitions := range producedOffsets {
		for partition, producedOffset := range partitions {
			committedOffset := committedOffsets[topic][partition]
			lag := producedOffset - committedOffset
			log.Printf("Offset Lag - Group: %s, Topic: %s, Partition: %d, Offset Lag: %d", groupName, topic, partition, lag)

			// Fetch the interpolation table from Redis
			interpolationData := fetchInterpolationData(redisClient, topic, partition)
			if len(interpolationData) < 2 {
				log.Printf("Not enough data to calculate time lag for Group: %s, Topic: %s, Partition: %d", groupName, topic, partition)
				continue
			}

			log.Printf("Interpolation Data for Group: %s, Topic: %s, Partition: %d: %+v", groupName, topic, partition, interpolationData)

			// Calculate the production timestamp for the last consumed offset
			predictedTimestamp := interpolateTimestamp(committedOffset, interpolationData)
			log.Printf("Predicted Timestamp for Group: %s, Topic: %s, Partition: %d, Committed Offset: %d, Predicted Timestamp: %d", groupName, topic, partition, committedOffset, predictedTimestamp)

			// Calculate time lag
			timeLag := currentTime - predictedTimestamp

			log.Printf("Time Lag (seconds) - Group: %s, Topic: %s, Partition: %d, Time Lag: %d", groupName, topic, partition, timeLag/1000)

			// Record the lag in Prometheus
			KafkaLagOffset.With(prometheus.Labels{
				"group":     groupName,
				"topic":     topic,
				"partition": strconv.Itoa(int(partition)),
			}).Set(float64(lag))

			KafkaTimeLag.With(prometheus.Labels{
				"group":     groupName,
				"topic":     topic,
				"partition": strconv.Itoa(int(partition)),
			}).Set(float64(timeLag) / 1000.0) // Time lag in seconds
		}
	}
}

func interpolateTimestamp(consumedOffset int64, interpolationTable []structs.OffsetTimestamp) int64 {
	var x1, x2, y1, y2 int64

	for i := 0; i < len(interpolationTable)-1; i++ {
		if interpolationTable[i].Offset <= consumedOffset && consumedOffset <= interpolationTable[i+1].Offset {
			x1, y1 = interpolationTable[i].Timestamp, interpolationTable[i].Offset
			x2, y2 = interpolationTable[i+1].Timestamp, interpolationTable[i+1].Offset
			if y2 == y1 {
				log.Printf("Warning: Offsets for interpolation points are identical, using latest timestamp x2: %d", interpolationTable[i+1].Timestamp)
				return interpolationTable[i+1].Timestamp
			}
			log.Printf("Interpolating between points: (%d, %d) and (%d, %d) for offset %d", y1, x1, y2, x2, consumedOffset)
			return x2 - (y2-consumedOffset)*(x2-x1)/(y2-y1)
		}
	}

	// If the loop didn't return, check if the last two entries have the same offset
	if len(interpolationTable) > 1 && interpolationTable[len(interpolationTable)-1].Offset == interpolationTable[len(interpolationTable)-2].Offset {
		log.Printf("Using latest timestamp for repeated offset: %d", interpolationTable[len(interpolationTable)-1].Timestamp)
		return interpolationTable[len(interpolationTable)-1].Timestamp
	}

	// If no points found for interpolation, use extrapolation with the first and last points
	x1, y1 = interpolationTable[0].Timestamp, interpolationTable[0].Offset
	x2, y2 = interpolationTable[len(interpolationTable)-1].Timestamp, interpolationTable[len(interpolationTable)-1].Offset
	if y2 == y1 {
		log.Printf("Warning: Offsets for extrapolation points are identical, using latest timestamp x2: %d", interpolationTable[len(interpolationTable)-1].Timestamp)
		return interpolationTable[len(interpolationTable)-1].Timestamp
	}
	log.Printf("Extrapolating between points: (%d, %d) and (%d, %d) for offset %d", y1, x1, y2, x2, consumedOffset)
	return x2 - (y2-consumedOffset)*(x2-x1)/(y2-y1)
}

func fetchInterpolationData(redisClient *redis.Client, topic string, partition int32) []structs.OffsetTimestamp {
	key := fmt.Sprintf("%s:%d", topic, partition)
	data, err := redisClient.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		log.Printf("Error fetching interpolation data from Redis for %s:%d: %v", topic, partition, err)
		return nil
	}

	log.Printf("Fetched raw data from Redis for %s:%d: %+v", topic, partition, data)

	// Use a map to store the latest timestamp for each offset
	offsetMap := make(map[int64]structs.OffsetTimestamp)
	for _, item := range data {
		timestampStr := item.Member.(string)
		offset := int64(item.Score)

		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			log.Printf("Error parsing timestamp: %v", err)
			continue
		}

		// Update the map to keep the latest timestamp for each offset
		if existing, found := offsetMap[offset]; !found || existing.Timestamp < timestamp {
			offsetMap[offset] = structs.OffsetTimestamp{
				Timestamp: timestamp,
				Offset:    offset,
			}
		}
	}

	// Convert the map back to a sorted slice
	var interpolationTable []structs.OffsetTimestamp
	for _, v := range offsetMap {
		interpolationTable = append(interpolationTable, v)
	}

	// Sort the slice by offset
	sort.Slice(interpolationTable, func(i, j int) bool {
		return interpolationTable[i].Offset < interpolationTable[j].Offset
	})

	log.Printf("Final interpolation data for %s:%d - %+v", topic, partition, interpolationTable)
	return interpolationTable
}
