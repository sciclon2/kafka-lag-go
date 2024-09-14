package metrics

import (
	"strconv"
	"sync"
	"time"

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
	totalGroupsChecked   *prometheus.GaugeVec // Total number of groups checked in each iteration
	iterationTimeSeconds prometheus.Gauge     // Time taken to complete the iteration For the whole taks
	extraLabels          map[string]string    // Additional labels for Prometheus metrics

}

func (pm *PrometheusMetrics) RegisterLocalMetrics() {
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

// NewLocalPrometheusMetrics creates a new PrometheusMetrics instance with the provided extra labels.
func NewLocalPrometheusMetrics(extraLabels map[string]string) *PrometheusMetrics {
	// Ensure labels map is not nil
	if extraLabels == nil {
		extraLabels = make(map[string]string)
	}

	// Define the base label keys
	baseLabelKeys := []string{"clustername", "group", "topic", "partition"}

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
			append([]string{"clustername", "group"}, keysFromMap(extraLabels)...), // Include extra labels
		),
		groupMaxLagInSeconds: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_group_max_lag_in_seconds",
				Help: "The maximum lag in seconds for a Kafka consumer group.",
			},
			append([]string{"clustername", "group"}, keysFromMap(extraLabels)...), // Include extra labels
		),
		topicMaxLagInOffsets: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_group_topic_max_lag_in_offsets",
				Help: "The maximum lag in offsets for a Kafka topic.",
			},
			append([]string{"clustername", "group", "topic"}, keysFromMap(extraLabels)...), // Include extra labels
		),
		topicMaxLagInSeconds: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_group_topic_max_lag_in_seconds",
				Help: "The maximum lag in seconds for a Kafka topic.",
			},
			append([]string{"clustername", "group", "topic"}, keysFromMap(extraLabels)...), // Include extra labels
		),
		groupSumLagInOffsets: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_group_sum_lag_in_offsets",
				Help: "The sum of lag in offsets for a Kafka consumer group.",
			},
			append([]string{"clustername", "group"}, keysFromMap(extraLabels)...), // Include extra labels
		),
		groupSumLagInSeconds: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_group_sum_lag_in_seconds",
				Help: "The sum of lag in seconds for a Kafka consumer group.",
			},
			append([]string{"clustername", "group"}, keysFromMap(extraLabels)...), // Include extra labels
		),
		topicSumLagInOffsets: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_group_topic_sum_lag_in_offsets",
				Help: "The sum of lag in offsets for a Kafka topic.",
			},
			append([]string{"clustername", "group", "topic"}, keysFromMap(extraLabels)...), // Include extra labels
		),
		topicSumLagInSeconds: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_consumer_group_topic_sum_lag_in_seconds",
				Help: "The sum of lag in seconds for a Kafka topic.",
			},
			append([]string{"clustername", "group", "topic"}, keysFromMap(extraLabels)...), // Include extra labels
		),
		iterationTimeSeconds: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "kafka_iteration_time_seconds",
				Help: "The time taken to complete an iteration of checking all consumer groups.",
			},
		),
		extraLabels: extraLabels, // Store the extra labels for use in processing
		totalGroupsChecked: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kafka_total_groups_checked",
				Help: "The total number of consumer groups checked in each iteration.",
			},
			append([]string{"clustername"}, keysFromMap(extraLabels)...), // Include extra labels

		),
	}
}

func (pm *PrometheusMetrics) ProcessLocalMetrics(metricsToExportChan <-chan *structs.Group, numWorkers int, startTime time.Time) {
	var wg sync.WaitGroup
	clusterGroupCountMap := sync.Map{} // Use sync.Map for concurrent access

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for group := range metricsToExportChan {
				// Include clustername in the base labels for all metrics
				baseLabels := prometheus.Labels{
					"clustername": group.ClusterName,
					"group":       group.Name,
				}

				for _, topic := range group.Topics {
					for _, partition := range topic.Partitions {
						// Prepare the labels for this metric, including partition
						partitionLabels := prometheus.Labels{
							"topic":     topic.Name,
							"partition": strconv.Itoa(int(partition.Number)),
						}
						// Merge with base labels and extra labels
						//finalLabels := MergeLabels(MergeLabels(baseLabels, partitionLabels), pm.extraLabels)
						finalLabels := MergeLabels(baseLabels, partitionLabels, pm.extraLabels)

						if partition.LagInOffsets != -1 {
							pm.lagInOffsets.With(finalLabels).Set(float64(partition.LagInOffsets))
						}
						if partition.LagInSeconds != -1 {
							pm.lagInSeconds.With(finalLabels).Set(float64(partition.LagInSeconds))
						}

						logrus.Debugf(
							"Set metrics for cluster: %s, group: %s, topic: %s, partition: %d, LagInOffsets: %d, LagInSeconds: %d",
							group.ClusterName, group.Name, topic.Name, partition.Number,
							partition.LagInOffsets, partition.LagInSeconds,
						)
					}

					// Set the max and sum metrics for the topic
					if topic.MaxLagInOffsets != -1 {
						pm.topicMaxLagInOffsets.With(MergeLabels(baseLabels, prometheus.Labels{"topic": topic.Name}, pm.extraLabels)).Set(float64(topic.MaxLagInOffsets))
						//pm.topicMaxLagInOffsets.With(MergeLabels(prometheus.Labels{"clustername": group.Clus"group": group.Name, "topic": topic.Name}, pm.extraLabels)).Set(float64(topic.MaxLagInOffsets))

					}
					if topic.MaxLagInSeconds != -1 {
						pm.topicMaxLagInSeconds.With(MergeLabels(baseLabels, prometheus.Labels{"topic": topic.Name}, pm.extraLabels)).Set(float64(topic.MaxLagInSeconds))
					}
					if topic.SumLagInOffsets != -1 {
						pm.topicSumLagInOffsets.With(MergeLabels(baseLabels, prometheus.Labels{"topic": topic.Name}, pm.extraLabels)).Set(float64(topic.SumLagInOffsets))
					}
					if topic.SumLagInSeconds != -1 {
						pm.topicSumLagInSeconds.With(MergeLabels(baseLabels, prometheus.Labels{"topic": topic.Name}, pm.extraLabels)).Set(float64(topic.SumLagInSeconds))
					}

					logrus.Debugf(
						"Set max and sum metrics for topic: %s, cluster: %s, group: %s, MaxLagInOffsets: %d, MaxLagInSeconds: %d, SumLagInOffsets: %d, SumLagInSeconds: %d",
						topic.Name, group.ClusterName, group.Name,
						topic.MaxLagInOffsets, topic.MaxLagInSeconds,
						topic.SumLagInOffsets, topic.SumLagInSeconds,
					)
				}

				// Set the maximum and sum lag metrics for the group
				if group.MaxLagInOffsets != -1 {
					pm.groupMaxLagInOffsets.With(MergeLabels(baseLabels, pm.extraLabels)).Set(float64(group.MaxLagInOffsets))
				}
				if group.MaxLagInSeconds != -1 {
					pm.groupMaxLagInSeconds.With(MergeLabels(baseLabels, pm.extraLabels)).Set(float64(group.MaxLagInSeconds))
				}
				if group.SumLagInOffsets != -1 {
					pm.groupSumLagInOffsets.With(MergeLabels(baseLabels, pm.extraLabels)).Set(float64(group.SumLagInOffsets))
				}
				if group.SumLagInSeconds != -1 {
					pm.groupSumLagInSeconds.With(MergeLabels(baseLabels, pm.extraLabels)).Set(float64(group.SumLagInSeconds))
				}

				logrus.Debugf(
					"Set max and sum metrics for group: %s, cluster: %s, MaxLagInOffsets: %d, MaxLagInSeconds: %d, SumLagInOffsets: %d, SumLagInSeconds: %d",
					group.Name, group.ClusterName,
					group.MaxLagInOffsets, group.MaxLagInSeconds,
					group.SumLagInOffsets, group.SumLagInSeconds,
				)

				// Increment the group count for the specific cluster in a thread-safe way
				currentValue, _ := clusterGroupCountMap.LoadOrStore(group.ClusterName, int64(0))
				clusterGroupCountMap.Store(group.ClusterName, currentValue.(int64)+1)
			}
		}()
	}

	wg.Wait()

	// Update the Prometheus metric for each cluster after all goroutines have finished
	clusterGroupCountMap.Range(func(key, value interface{}) bool {
		clusterName := key.(string)
		groupCount := value.(int64)
		pm.totalGroupsChecked.With(MergeLabels(prometheus.Labels{"clustername": clusterName}, pm.extraLabels)).Set(float64(groupCount))
		logrus.Infof("Total groups checked for cluster %s: %d", clusterName, groupCount)
		return true
	})

	// Calculate and set the iteration time
	iterationTime := time.Since(startTime).Seconds()
	pm.iterationTimeSeconds.Set(iterationTime)
	logrus.Infof("Iteration time: %.2f seconds", iterationTime)
}

// MergeLabels merges the base labels with extra labels.
func MergeLabels(labelMaps ...prometheus.Labels) prometheus.Labels {
	mergedLabels := prometheus.Labels{}

	for _, labels := range labelMaps {
		for key, value := range labels {
			mergedLabels[key] = value
		}
	}

	return mergedLabels
}

// Helper function to extract keys from a map
func keysFromMap(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
