package metrics

import (
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/redis/go-redis/v9"
	"github.com/sciclon2/kafka-lag-go/pkg/structs"
	"github.com/sirupsen/logrus"
)

// LagProcessor processes and records lag metrics for Kafka consumer groups.
type LagProcessor struct{}

// NewLagProcessor creates a new instance of LagProcessor.
func NewLagProcessor() *LagProcessor {
	return &LagProcessor{}
}

func (lp *LagProcessor) processGroup(group *structs.Group) {
	// Initialize temporary sum variables to track totals across topics
	var tempSumLagInOffsets, tempSumLagInSeconds int64
	var validOffsetsFound, validSecondsFound bool

	logrus.Debugf("Starting to process group: %s", group.Name)

	for tIndex := range group.Topics {
		topic := &group.Topics[tIndex]
		logrus.Debugf("Processing topic: %s for group: %s", topic.Name, group.Name)
		// Process each topic and update the group sums
		lp.processTopic(topic, group, &tempSumLagInOffsets, &tempSumLagInSeconds, &validOffsetsFound, &validSecondsFound)
	}

	// Set the group's sum values
	lp.updateGroupSums(group, tempSumLagInOffsets, tempSumLagInSeconds, validOffsetsFound, validSecondsFound)

	logrus.Infof("Finished processing group: %s in cluster: %s, MaxLagInOffsets: %d, MaxLagInSeconds: %d, SumLagInOffsets: %d, SumLagInSeconds: %d",
		group.Name, group.ClusterName, group.MaxLagInOffsets, group.MaxLagInSeconds, group.SumLagInOffsets, group.SumLagInSeconds)
}

func (lp *LagProcessor) processTopic(topic *structs.Topic, group *structs.Group, tempSumLagInOffsets, tempSumLagInSeconds *int64, validOffsetsFound, validSecondsFound *bool) {
	// Initialize temporary topic sum variables as zero
	var tempTopicSumLagInOffsets, tempTopicSumLagInSeconds int64
	var topicValidOffsetsFound, topicValidSecondsFound bool

	// Initialize max values for the topic
	topic.MaxLagInOffsets = -1
	topic.MaxLagInSeconds = -1

	logrus.Debugf("Starting to process topic: %s", topic.Name)

	for pIndex := range topic.Partitions {
		partition := &topic.Partitions[pIndex]
		logrus.Debugf("Processing Partition: %d for Topic: %s", partition.Number, topic.Name)

		if partition.LatestProducedOffset != -1 && partition.CommitedOffset != -1 {
			// Calculate lags and update max values
			lp.calculateAndAccumulateLags(partition, group, topic, &tempTopicSumLagInOffsets, &tempTopicSumLagInSeconds, &topicValidOffsetsFound, &topicValidSecondsFound)
		} else {
			logMessage := fmt.Sprintf("Skipped Partition: %d for Topic: %s due to missing offsets:", partition.Number, topic.Name)
			if partition.LatestProducedOffset == -1 {
				logMessage += " LatestProducedOffset is not available."
			}
			if partition.CommitedOffset == -1 {
				logMessage += " CommitedOffset is not available."
			}
			logrus.Debug(logMessage)
		}
	}

	// Set the topic's sum values
	lp.updateTopicSums(topic, tempTopicSumLagInOffsets, tempTopicSumLagInSeconds, topicValidOffsetsFound, topicValidSecondsFound)

	// Accumulate the topic sums into the group sums
	if topicValidOffsetsFound {
		*tempSumLagInOffsets += tempTopicSumLagInOffsets
		*validOffsetsFound = true
	}
	if topicValidSecondsFound {
		*tempSumLagInSeconds += tempTopicSumLagInSeconds
		*validSecondsFound = true
	}

	logrus.Debugf("Finished processing topic: %s, MaxLagInOffsets: %d, MaxLagInSeconds: %d, SumLagInOffsets: %d, SumLagInSeconds: %d",
		topic.Name, topic.MaxLagInOffsets, topic.MaxLagInSeconds, topic.SumLagInOffsets, topic.SumLagInSeconds)
}

// calculateAndAccumulateLags calculates the lags for a partition and updates the topic's and group's maximum and sum values.
func (lp *LagProcessor) calculateAndAccumulateLags(
	partition *structs.Partition,
	group *structs.Group,
	topic *structs.Topic,
	tempTopicSumLagInOffsets, tempTopicSumLagInSeconds *int64,
	topicValidOffsetsFound, topicValidSecondsFound *bool) {

	lp.calculateOffsetLag(partition, group)
	lp.calculateTimeLag(partition, group)

	logrus.Debugf("Processing Partition %d, Topic %s: LagInOffsets=%d, LagInSeconds=%d",
		partition.Number, topic.Name, partition.LagInOffsets, partition.LagInSeconds)

	// Only accumulate the lag if it's a valid value (not -1)
	if partition.LagInOffsets != -1 {
		*tempTopicSumLagInOffsets += partition.LagInOffsets
		*topicValidOffsetsFound = true

		// Update topic's max lag if needed
		if topic.MaxLagInOffsets == -1 || partition.LagInOffsets > topic.MaxLagInOffsets {
			logrus.Debugf("Updating MaxLagInOffsets for topic: %s, old value: %d, new value: %d",
				topic.Name, topic.MaxLagInOffsets, partition.LagInOffsets)
			topic.MaxLagInOffsets = partition.LagInOffsets
		}
	}

	if partition.LagInSeconds != -1 {
		*tempTopicSumLagInSeconds += partition.LagInSeconds
		*topicValidSecondsFound = true

		// Update topic's max lag if needed
		if topic.MaxLagInSeconds == -1 || partition.LagInSeconds > topic.MaxLagInSeconds {
			logrus.Debugf("Updating MaxLagInSeconds for topic: %s, old value: %d, new value: %d",
				topic.Name, topic.MaxLagInSeconds, partition.LagInSeconds)
			topic.MaxLagInSeconds = partition.LagInSeconds
		}
	}

	// Update group's max lag if needed
	if partition.LagInOffsets > group.MaxLagInOffsets {
		logrus.Debugf("Updating MaxLagInOffsets for group: %s, old value: %d, new value: %d",
			group.Name, group.MaxLagInOffsets, partition.LagInOffsets)
		group.MaxLagInOffsets = partition.LagInOffsets
	} else {
		logrus.Debugf("Skipping update for MaxLagInOffsets for group: %s, current value: %d, partition value: %d",
			group.Name, group.MaxLagInOffsets, partition.LagInOffsets)
	}

	if partition.LagInSeconds > group.MaxLagInSeconds {
		logrus.Debugf("Updating MaxLagInSeconds for group: %s, old value: %d, new value: %d",
			group.Name, group.MaxLagInSeconds, partition.LagInSeconds)
		group.MaxLagInSeconds = partition.LagInSeconds
	} else {
		logrus.Debugf("Skipping update for MaxLagInSeconds for group: %s, current value: %d, partition value: %d",
			group.Name, group.MaxLagInSeconds, partition.LagInSeconds)
	}
}

func (lp *LagProcessor) updateTopicSums(topic *structs.Topic, tempTopicSumLagInOffsets, tempTopicSumLagInSeconds int64, validOffsetsFound, validSecondsFound bool) {
	// Set the topic's sum values to the accumulated sums or -1 if no valid sums were found
	if validOffsetsFound {
		topic.SumLagInOffsets = tempTopicSumLagInOffsets
	} else {
		topic.SumLagInOffsets = -1
	}

	if validSecondsFound {
		topic.SumLagInSeconds = tempTopicSumLagInSeconds
	} else {
		topic.SumLagInSeconds = -1
	}

	logrus.Debugf("Final sums for topic: %s, SumLagInOffsets: %d, SumLagInSeconds: %d",
		topic.Name, topic.SumLagInOffsets, topic.SumLagInSeconds)
}

func (lp *LagProcessor) updateGroupSums(group *structs.Group, tempSumLagInOffsets, tempSumLagInSeconds int64, validOffsetsFound, validSecondsFound bool) {
	// Set the group's sum values to the accumulated sums or -1 if no valid sums were found
	if validOffsetsFound {
		group.SumLagInOffsets = tempSumLagInOffsets
	} else {
		group.SumLagInOffsets = -1
	}

	if validSecondsFound {
		group.SumLagInSeconds = tempSumLagInSeconds
	} else {
		group.SumLagInSeconds = -1
	}

	logrus.Debugf("Final sums for group: %s, SumLagInOffsets: %d, SumLagInSeconds: %d",
		group.Name, group.SumLagInOffsets, group.SumLagInSeconds)
}

// calculateOffsetLag calculates the lag in offsets and updates the partition's LagInOffsets field.
func (lp *LagProcessor) calculateOffsetLag(partition *structs.Partition, group *structs.Group) {
	lagInOffsets := partition.LatestProducedOffset - partition.CommitedOffset

	// Ensure lag is non-negative
	if lagInOffsets < 0 {
		lagInOffsets = 0
	}
	partition.LagInOffsets = lagInOffsets

	// Update group's max lag in offsets
	if lagInOffsets > group.MaxLagInOffsets {
		group.MaxLagInOffsets = lagInOffsets
	}
}

// calculateTimeLag calculates the time lag in seconds and updates the partition's LagInSeconds field.
func (lp *LagProcessor) calculateTimeLag(partition *structs.Partition, group *structs.Group) {
	lagInSeconds, err := lp.computeTimeLag(&partition.ProducedOffsetsHistory, partition.CommitedOffset, partition.LatestProducedOffsetAt)
	if err != nil {
		logrus.Debugf("Unable to calculate time lag for Partition: %d, Error: %v", partition.Number, err)
		return
	}

	partition.LagInSeconds = lagInSeconds

	// Update group's max lag in seconds
	if lagInSeconds > group.MaxLagInSeconds {
		group.MaxLagInSeconds = lagInSeconds
	}
}

// computeTimeLag calculates the time lag based on the produced offsets history using interpolation or extrapolation.
func (lp *LagProcessor) computeTimeLag(offsetsHistory *[]redis.Z, lastConsumedOffset, latestProducedOffsetAt int64) (int64, error) {
	lowerOffset, lowerTimestamp, upperOffset, upperTimestamp, err := lp.findNearestOffsets(offsetsHistory, lastConsumedOffset, latestProducedOffsetAt)
	if err != nil {
		return 0, err
	}
	estimatedTimestamp := lp.interpolateOrExtrapolateTimestamp(lastConsumedOffset, lowerOffset, lowerTimestamp, upperOffset, upperTimestamp)
	timeLagSeconds := (latestProducedOffsetAt - estimatedTimestamp) / 1000

	return timeLagSeconds, nil
}

// findNearestOffsets retrieves the offsets closest to the last consumed offset for interpolation or extrapolation.
func (lp *LagProcessor) findNearestOffsets(offsetsHistory *[]redis.Z, lastConsumedOffset, latestProducedOffsetAt int64) (int64, int64, int64, int64, error) {
	if len(*offsetsHistory) < 2 {
		return 0, 0, 0, 0, errors.New("insufficient data points for interpolation or extrapolation")
	}

	var lowerOffset, upperOffset int64
	var lowerTimestamp, upperTimestamp int64
	foundLower := false

	for _, val := range *offsetsHistory {
		offset := int64(val.Score)

		timestampStr, ok := val.Member.(string)
		if !ok {
			return 0, 0, 0, 0, errors.New("invalid timestamp format")
		}

		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			return 0, 0, 0, 0, errors.New("invalid timestamp string format")
		}

		if offset <= lastConsumedOffset {
			lowerOffset = offset
			lowerTimestamp = timestamp
			foundLower = true

			if offset == lastConsumedOffset {
				upperOffset = lowerOffset
				upperTimestamp = lowerTimestamp
				return lowerOffset, lowerTimestamp, upperOffset, upperTimestamp, nil
			}
		} else if offset > lastConsumedOffset && !foundLower {
			upperOffset = offset
			upperTimestamp = timestamp
			break
		} else if offset > lastConsumedOffset {
			upperOffset = offset
			upperTimestamp = timestamp
			break
		}
	}

	if !foundLower {
		lowerOffset = upperOffset
		lowerTimestamp = upperTimestamp
		upperTimestamp = latestProducedOffsetAt
	}
	if upperOffset == 0 {
		upperOffset = lowerOffset
		upperTimestamp = latestProducedOffsetAt
	}

	return lowerOffset, lowerTimestamp, upperOffset, upperTimestamp, nil
}

// interpolateOrExtrapolateTimestamp calculates the interpolated or extrapolated timestamp for the last consumed offset.
func (lp *LagProcessor) interpolateOrExtrapolateTimestamp(lastConsumedOffset, lowerOffset, lowerTimestamp, upperOffset, upperTimestamp int64) int64 {
	if lowerOffset == lastConsumedOffset {
		return lowerTimestamp
	} else if upperOffset == lastConsumedOffset {
		return upperTimestamp
	}

	if lowerOffset == upperOffset {
		return lowerTimestamp
	}

	deltaOffset := upperOffset - lowerOffset
	deltaTime := upperTimestamp - lowerTimestamp

	return lowerTimestamp + ((lastConsumedOffset - lowerOffset) * deltaTime / deltaOffset)
}

// GenerateMetrics processes groups from the groupStructCompleteAndPersistedChan channel,
// calculates lag metrics, and returns a channel with the processed data.
func (lp *LagProcessor) GenerateMetrics(groupsComplete <-chan *structs.Group, numWorkers int) <-chan *structs.Group {
	metricsToExportChan := make(chan *structs.Group)

	var wg sync.WaitGroup

	// Start multiple goroutines to process the groups concurrently
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for group := range groupsComplete {
				// Process the group to calculate lag metrics
				lp.processGroup(group)

				// Send the processed group to the output channel
				metricsToExportChan <- group
			}
		}()
	}

	// Close the channel after all workers are done
	go func() {
		wg.Wait()
		close(metricsToExportChan)
	}()

	return metricsToExportChan
}
