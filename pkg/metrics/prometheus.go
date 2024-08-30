package metrics

import (
	"strconv"
	"sync"
	"time"

	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sciclon2/kafka-lag-go/pkg/structs"
	"github.com/sirupsen/logrus"
)

// PrometheusMetrics holds the Prometheus metrics to be exposed
type PrometheusMetrics struct {
	lagInOffsets         *prometheus.GaugeVec
	lagInSeconds         *prometheus.GaugeVec
	groupMaxLagInOffsets *prometheus.GaugeVec // Maximum lag in offsets at the group level
	groupMaxLagInSeconds *prometheus.GaugeVec // Maximum lag in seconds at the group level
	topicMaxLagInOffsets *prometheus.GaugeVec // Maximum lag in offsets at the topic level
	topicMaxLagInSeconds *prometheus.GaugeVec // Maximum lag in seconds at the topic level
	groupSumLagInOffsets *prometheus.GaugeVec // Sum of lag in offsets at the group level
	groupSumLagInSeconds *prometheus.GaugeVec // Sum of lag in seconds at the group level
	topicSumLagInOffsets *prometheus.GaugeVec // Sum of lag in offsets at the topic level
	topicSumLagInSeconds *prometheus.GaugeVec // Sum of lag in seconds at the topic level
	totalGroupsChecked   prometheus.Gauge     // Total number of groups checked in each iteration
	iterationTimeSeconds prometheus.Gauge     // Time taken to complete the iteration For the whole taks
	extraLabels          map[string]string    // Additional labels for Prometheus metrics

}

func NewPrometheusMetrics(extraLabels map[string]string) *PrometheusMetrics {
	// Ensure labels map is not nil
	if extraLabels == nil {
		extraLabels = make(map[string]string)
	}

	// Define the base label keys
	baseLabelKeys := []string{"group", "topic", "partition"}

	// Dynamically add extra label keys
	for key := range extraLabels {
		baseLabelKeys = append(baseLabelKeys, key)
	}

	return &PrometheusMetrics{
		lagInOffsets: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_group_lag_in_offsets",
				Help: "The lag in offsets for a Kafka consumer group.",
			},
			baseLabelKeys,
		),
		lagInSeconds: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_group_lag_in_seconds",
				Help: "The lag in seconds for a Kafka consumer group.",
			},
			baseLabelKeys,
		),
		groupMaxLagInOffsets: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_group_max_lag_in_offsets",
				Help: "The maximum lag in offsets for a Kafka consumer group.",
			},
			append([]string{"group"}, keysFromMap(extraLabels)...), // Include extra labels
		),
		groupMaxLagInSeconds: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_group_max_lag_in_seconds",
				Help: "The maximum lag in seconds for a Kafka consumer group.",
			},
			append([]string{"group"}, keysFromMap(extraLabels)...), // Include extra labels
		),
		topicMaxLagInOffsets: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_group_topic_max_lag_in_offsets",
				Help: "The maximum lag in offsets for a Kafka topic.",
			},
			append([]string{"group", "topic"}, keysFromMap(extraLabels)...), // Include extra labels
		),
		topicMaxLagInSeconds: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_group_topic_max_lag_in_seconds",
				Help: "The maximum lag in seconds for a Kafka topic.",
			},
			append([]string{"group", "topic"}, keysFromMap(extraLabels)...), // Include extra labels
		),
		groupSumLagInOffsets: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_group_sum_lag_in_offsets",
				Help: "The sum of lag in offsets for a Kafka consumer group.",
			},
			append([]string{"group"}, keysFromMap(extraLabels)...), // Include extra labels
		),
		groupSumLagInSeconds: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_group_sum_lag_in_seconds",
				Help: "The sum of lag in seconds for a Kafka consumer group.",
			},
			append([]string{"group"}, keysFromMap(extraLabels)...), // Include extra labels
		),
		topicSumLagInOffsets: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_group_topic_sum_lag_in_offsets",
				Help: "The sum of lag in offsets for a Kafka topic.",
			},
			append([]string{"group", "topic"}, keysFromMap(extraLabels)...), // Include extra labels
		),
		topicSumLagInSeconds: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_group_topic_sum_lag_in_seconds",
				Help: "The sum of lag in seconds for a Kafka topic.",
			},
			append([]string{"group", "topic"}, keysFromMap(extraLabels)...), // Include extra labels
		),
		totalGroupsChecked: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "kafka_total_groups_checked",
				Help: "The total number of consumer groups checked in each iteration.",
			},
		),
		iterationTimeSeconds: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "kafka_iteration_time_seconds",
				Help: "The time taken to complete an iteration of checking all consumer groups.",
			},
		),
		extraLabels: extraLabels, // Store the extra labels for use in processing
	}
}
func (pm *PrometheusMetrics) RegisterMetrics() {
	prometheus.MustRegister(pm.lagInOffsets)
	prometheus.MustRegister(pm.lagInSeconds)
	prometheus.MustRegister(pm.groupMaxLagInOffsets)
	prometheus.MustRegister(pm.groupMaxLagInSeconds)
	prometheus.MustRegister(pm.topicMaxLagInOffsets)
	prometheus.MustRegister(pm.topicMaxLagInSeconds)
	prometheus.MustRegister(pm.groupSumLagInOffsets)
	prometheus.MustRegister(pm.groupSumLagInSeconds)
	prometheus.MustRegister(pm.topicSumLagInOffsets)
	prometheus.MustRegister(pm.topicSumLagInSeconds)
	prometheus.MustRegister(pm.totalGroupsChecked)
	prometheus.MustRegister(pm.iterationTimeSeconds)
}

func (pm *PrometheusMetrics) ProcessMetrics(metricsToExportChan <-chan *structs.Group, numWorkers int, startTime time.Time) {
	var wg sync.WaitGroup
	var totalGroupsChecked int64 // Use int64 for atomic operations

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			workerGroupCount := int64(0) // Local counter for this goroutine

			for group := range metricsToExportChan {

				for _, topic := range group.Topics {
					for _, partition := range topic.Partitions {
						// Prepare the base labels for this metric
						baseLabels := prometheus.Labels{
							"group":     group.Name,
							"topic":     topic.Name,
							"partition": strconv.Itoa(int(partition.Number)),
						}

						// Merge with extra labels
						finalLabels := MergeLabels(baseLabels, pm.extraLabels)

						if partition.LagInOffsets != -1 {
							pm.lagInOffsets.With(finalLabels).Set(float64(partition.LagInOffsets))
						}
						if partition.LagInSeconds != -1 {
							pm.lagInSeconds.With(finalLabels).Set(float64(partition.LagInSeconds))
						}

						logrus.Debugf(
							"Set metrics for group: %s, topic: %s, partition: %d, LagInOffsets: %d, LagInSeconds: %d",
							group.Name, topic.Name, partition.Number,
							partition.LagInOffsets, partition.LagInSeconds,
						)
					}

					// Set the max and sum metrics for the topic
					if topic.MaxLagInOffsets != -1 {
						pm.topicMaxLagInOffsets.With(MergeLabels(prometheus.Labels{"group": group.Name, "topic": topic.Name}, pm.extraLabels)).Set(float64(topic.MaxLagInOffsets))
					}
					if topic.MaxLagInSeconds != -1 {
						pm.topicMaxLagInSeconds.With(MergeLabels(prometheus.Labels{"group": group.Name, "topic": topic.Name}, pm.extraLabels)).Set(float64(topic.MaxLagInSeconds))
					}
					if topic.SumLagInOffsets != -1 {
						pm.topicSumLagInOffsets.With(MergeLabels(prometheus.Labels{"group": group.Name, "topic": topic.Name}, pm.extraLabels)).Set(float64(topic.SumLagInOffsets))
					}
					if topic.SumLagInSeconds != -1 {
						pm.topicSumLagInSeconds.With(MergeLabels(prometheus.Labels{"group": group.Name, "topic": topic.Name}, pm.extraLabels)).Set(float64(topic.SumLagInSeconds))
					}

					logrus.Debugf(
						"Set max and sum metrics for topic: %s, group: %s, MaxLagInOffsets: %d, MaxLagInSeconds: %d, SumLagInOffsets: %d, SumLagInSeconds: %d",
						topic.Name, group.Name,
						topic.MaxLagInOffsets, topic.MaxLagInSeconds,
						topic.SumLagInOffsets, topic.SumLagInSeconds,
					)
				}

				// Set the maximum and sum lag metrics for the group
				if group.MaxLagInOffsets != -1 {
					pm.groupMaxLagInOffsets.With(MergeLabels(prometheus.Labels{"group": group.Name}, pm.extraLabels)).Set(float64(group.MaxLagInOffsets))
				}
				if group.MaxLagInSeconds != -1 {
					pm.groupMaxLagInSeconds.With(MergeLabels(prometheus.Labels{"group": group.Name}, pm.extraLabels)).Set(float64(group.MaxLagInSeconds))
				}
				if group.SumLagInOffsets != -1 {
					pm.groupSumLagInOffsets.With(MergeLabels(prometheus.Labels{"group": group.Name}, pm.extraLabels)).Set(float64(group.SumLagInOffsets))
				}
				if group.SumLagInSeconds != -1 {
					pm.groupSumLagInSeconds.With(MergeLabels(prometheus.Labels{"group": group.Name}, pm.extraLabels)).Set(float64(group.SumLagInSeconds))
				}

				logrus.Debugf(
					"Set max and sum metrics for group: %s, MaxLagInOffsets: %d, MaxLagInSeconds: %d, SumLagInOffsets: %d, SumLagInSeconds: %d",
					group.Name, group.MaxLagInOffsets, group.MaxLagInSeconds,
					group.SumLagInOffsets, group.SumLagInSeconds,
				)

				workerGroupCount++ // Increment the local counter
			}

			// Atomically add the local count to the total
			atomic.AddInt64(&totalGroupsChecked, workerGroupCount)
		}()
	}

	wg.Wait()

	// Update the Prometheus metric once after all goroutines have finished
	pm.totalGroupsChecked.Add(float64(totalGroupsChecked))
	logrus.Infof("Total groups checked: %d", totalGroupsChecked)

	// Calculate and set the iteration time
	iterationTime := time.Since(startTime).Seconds()
	pm.iterationTimeSeconds.Set(iterationTime)
	logrus.Infof("Iteration time: %.2f seconds", iterationTime)
}

// MergeLabels merges the base labels with extra labels.
func MergeLabels(baseLabels prometheus.Labels, extraLabels map[string]string) prometheus.Labels {
	// Add all extra labels
	for key, value := range extraLabels {
		baseLabels[key] = value
	}
	return baseLabels
}

// Helper function to extract keys from a map
func keysFromMap(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
