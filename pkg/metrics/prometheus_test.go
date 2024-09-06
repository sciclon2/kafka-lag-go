package metrics

import (
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sciclon2/kafka-lag-go/pkg/structs"
	"github.com/stretchr/testify/assert"
)

func setupPrometheusMetricsWithRegistry(registry *prometheus.Registry) *PrometheusMetrics {
	extraLabels := map[string]string{
		"env":     "test",
		"version": "v1",
	}
	pm := NewPrometheusMetrics(extraLabels)

	// Register metrics with the provided registry
	registry.MustRegister(pm.lagInOffsets)
	registry.MustRegister(pm.lagInSeconds)
	registry.MustRegister(pm.groupMaxLagInOffsets)
	registry.MustRegister(pm.groupMaxLagInSeconds)
	registry.MustRegister(pm.topicMaxLagInOffsets)
	registry.MustRegister(pm.topicMaxLagInSeconds)
	registry.MustRegister(pm.groupSumLagInOffsets)
	registry.MustRegister(pm.groupSumLagInSeconds)
	registry.MustRegister(pm.topicSumLagInOffsets)
	registry.MustRegister(pm.topicSumLagInSeconds)
	registry.MustRegister(pm.iterationTimeSeconds)
	registry.MustRegister(pm.totalGroupsChecked)

	return pm
}

func TestPrometheusMetrics_ProcessMetrics(t *testing.T) {
	// Create a new registry for the test
	registry := prometheus.NewRegistry()

	// Set up PrometheusMetrics with the registry and extra labels
	pm := setupPrometheusMetricsWithRegistry(registry)

	// Create a channel to export metrics
	metricsToExportChan := make(chan *structs.Group, 1)
	startTime := time.Now()

	// Create a test group with some metrics
	group := &structs.Group{
		Name:        "test-group",
		ClusterName: "test-cluster",

		Topics: []structs.Topic{
			{
				Name: "test-topic",
				Partitions: []structs.Partition{
					{Number: 0, LagInOffsets: 100, LagInSeconds: 200},
				},
				MaxLagInOffsets: 100,
				MaxLagInSeconds: 200,
				SumLagInOffsets: 100,
				SumLagInSeconds: 200,
			},
		},
		MaxLagInOffsets: 100,
		MaxLagInSeconds: 200,
		SumLagInOffsets: 100,
		SumLagInSeconds: 200,
	}

	// Send the test group into the channel and close it
	metricsToExportChan <- group
	close(metricsToExportChan)

	// Process the metrics
	pm.ProcessMetrics(metricsToExportChan, 1, startTime)

	// Validate that metrics are recorded using the custom registry

	// Expected labels for partition-based metrics
	expectedPartitionLabels := prometheus.Labels{
		"clustername": "test-cluster",
		"group":       "test-group",
		"topic":       "test-topic",
		"partition":   "0",
		"env":         "test",
		"version":     "v1",
	}

	// Validate lag in offsets and lag in seconds
	assert.Equal(t, 100.0, testutil.ToFloat64(pm.lagInOffsets.With(expectedPartitionLabels)))
	assert.Equal(t, 200.0, testutil.ToFloat64(pm.lagInSeconds.With(expectedPartitionLabels)))

	// Expected labels for group-level metrics (max and sum)
	expectedGroupLabels := prometheus.Labels{
		"clustername": "test-cluster",
		"group":       "test-group",
		"env":         "test",
		"version":     "v1",
	}

	// Validate group-level max and sum metrics
	assert.Equal(t, 100.0, testutil.ToFloat64(pm.groupMaxLagInOffsets.With(expectedGroupLabels)))
	assert.Equal(t, 200.0, testutil.ToFloat64(pm.groupMaxLagInSeconds.With(expectedGroupLabels)))
	assert.Equal(t, 100.0, testutil.ToFloat64(pm.groupSumLagInOffsets.With(expectedGroupLabels)))
	assert.Equal(t, 200.0, testutil.ToFloat64(pm.groupSumLagInSeconds.With(expectedGroupLabels)))

	// Check total groups checked
	expectedTotalGroupsLabels := prometheus.Labels{
		"clustername": "test-cluster",
		"env":         "test",
		"version":     "v1",
	}
	assert.Equal(t, 1.0, testutil.ToFloat64(pm.totalGroupsChecked.With(expectedTotalGroupsLabels)))
}

func TestIterationTimeCalculation(t *testing.T) {
	// Create a new registry for the test
	registry := prometheus.NewRegistry()
	pm := setupPrometheusMetricsWithRegistry(registry)
	startTime := time.Now()

	metricsToExportChan := make(chan *structs.Group, 1)

	// Send a dummy group to the channel
	dummyGroup := &structs.Group{
		Name: "dummy-group",
		Topics: []structs.Topic{
			{
				Name: "dummy-topic",
				Partitions: []structs.Partition{
					{Number: 0, LagInOffsets: 100, LagInSeconds: 200},
				},
				MaxLagInOffsets: 100,
				MaxLagInSeconds: 200,
				SumLagInOffsets: 100,
				SumLagInSeconds: 200,
			},
		},
		MaxLagInOffsets: 100,
		MaxLagInSeconds: 200,
		SumLagInOffsets: 100,
		SumLagInSeconds: 200,
	}

	metricsToExportChan <- dummyGroup
	close(metricsToExportChan) // Close the channel to simulate the end of metric collection

	// Simulate some delay
	time.Sleep(100 * time.Millisecond)

	pm.ProcessMetrics(metricsToExportChan, 1, startTime)

	// Ensure that the iteration time is recorded
	iterationTime := testutil.ToFloat64(pm.iterationTimeSeconds)

	// Allow for a wider range of time, e.g., between 0.09 and 0.2 seconds
	assert.Greater(t, iterationTime, 0.09)
	assert.Less(t, iterationTime, 0.2)
}

func TestProcessMetrics_EmptyChannel(t *testing.T) {
	// Create a new registry for the test
	registry := prometheus.NewRegistry()
	pm := setupPrometheusMetricsWithRegistry(registry)
	startTime := time.Now()

	metricsToExportChan := make(chan *structs.Group) // Empty channel
	close(metricsToExportChan)                       // Close immediately to simulate no metrics

	// Process the metrics
	pm.ProcessMetrics(metricsToExportChan, 1, startTime)

	// Ensure that no metrics have been recorded
	assert.Equal(t, 0.0, testutil.ToFloat64(pm.totalGroupsChecked.With(prometheus.Labels{"clustername": "test-cluster", "env": "test", "version": "v1"})))
}

func TestProcessMetrics_MultipleGroups(t *testing.T) {
	// Create a new registry for the test
	registry := prometheus.NewRegistry()
	pm := setupPrometheusMetricsWithRegistry(registry)
	startTime := time.Now()

	metricsToExportChan := make(chan *structs.Group, 2)

	// Create two test groups with different topics
	group1 := &structs.Group{
		Name:        "group1",
		ClusterName: "cluster1",
		Topics: []structs.Topic{
			{
				Name: "topic1",
				Partitions: []structs.Partition{
					{Number: 0, LagInOffsets: 100, LagInSeconds: 200},
					{Number: 1, LagInOffsets: 150, LagInSeconds: 250},
				},
				MaxLagInOffsets: 150,
				MaxLagInSeconds: 250,
				SumLagInOffsets: 250,
				SumLagInSeconds: 450,
			},
		},
		MaxLagInOffsets: 150,
		MaxLagInSeconds: 250,
		SumLagInOffsets: 250,
		SumLagInSeconds: 450,
	}

	group2 := &structs.Group{
		Name:        "group2",
		ClusterName: "cluster2",
		Topics: []structs.Topic{
			{
				Name: "topic2",
				Partitions: []structs.Partition{
					{Number: 0, LagInOffsets: 50, LagInSeconds: 100},
				},
				MaxLagInOffsets: 50,
				MaxLagInSeconds: 100,
				SumLagInOffsets: 50,
				SumLagInSeconds: 100,
			},
		},
		MaxLagInOffsets: 50,
		MaxLagInSeconds: 100,
		SumLagInOffsets: 50,
		SumLagInSeconds: 100,
	}

	metricsToExportChan <- group1
	metricsToExportChan <- group2
	close(metricsToExportChan)

	// Process the metrics
	pm.ProcessMetrics(metricsToExportChan, 2, startTime)

	// Validate metrics for group1
	expectedPartitionLabelsGroup1 := prometheus.Labels{
		"clustername": "cluster1",
		"group":       "group1",
		"topic":       "topic1",
		"partition":   "0",
		"env":         "test",
		"version":     "v1",
	}
	assert.Equal(t, 100.0, testutil.ToFloat64(pm.lagInOffsets.With(expectedPartitionLabelsGroup1)))

	// Validate metrics for group2
	expectedPartitionLabelsGroup2 := prometheus.Labels{
		"clustername": "cluster2",
		"group":       "group2",
		"topic":       "topic2",
		"partition":   "0",
		"env":         "test",
		"version":     "v1",
	}
	assert.Equal(t, 50.0, testutil.ToFloat64(pm.lagInOffsets.With(expectedPartitionLabelsGroup2)))
}

func TestPrometheusMetrics_RegistrationAndProcessing(t *testing.T) {
	registry := prometheus.NewRegistry()

	// Setup PrometheusMetrics and register metrics with the registry
	pm := setupPrometheusMetricsWithRegistry(registry)

	// Prepare mock data for Kafka consumer groups
	metricsToExportChan := make(chan *structs.Group, 1)

	// Create a test group with some metrics
	group := &structs.Group{
		Name:        "test-group",
		ClusterName: "test-cluster",
		Topics: []structs.Topic{
			{
				Name: "test-topic",
				Partitions: []structs.Partition{
					{Number: 0, LagInOffsets: 100, LagInSeconds: 200},
				},
				MaxLagInOffsets: 100,
				MaxLagInSeconds: 200,
				SumLagInOffsets: 100,
				SumLagInSeconds: 200,
			},
		},
		MaxLagInOffsets: 100,
		MaxLagInSeconds: 200,
		SumLagInOffsets: 100,
		SumLagInSeconds: 200,
	}

	// Send the test group into the channel and close it
	metricsToExportChan <- group
	close(metricsToExportChan)

	// Process the metrics
	startTime := time.Now()
	pm.ProcessMetrics(metricsToExportChan, 1, startTime)

	// Gather the metrics from the registry
	metricFamilies, err := registry.Gather()
	assert.NoError(t, err)

	// Define expected metrics and their labels
	expectedMetrics := map[string][]string{
		"kafka_consumer_group_lag_in_offsets":           {"clustername", "group", "topic", "partition", "env", "version"},
		"kafka_consumer_group_lag_in_seconds":           {"clustername", "group", "topic", "partition", "env", "version"},
		"kafka_consumer_group_max_lag_in_offsets":       {"clustername", "group", "env", "version"},
		"kafka_consumer_group_max_lag_in_seconds":       {"clustername", "group", "env", "version"},
		"kafka_consumer_group_sum_lag_in_offsets":       {"clustername", "group", "env", "version"},
		"kafka_consumer_group_sum_lag_in_seconds":       {"clustername", "group", "env", "version"},
		"kafka_consumer_group_topic_max_lag_in_offsets": {"clustername", "group", "topic", "env", "version"},
		"kafka_consumer_group_topic_max_lag_in_seconds": {"clustername", "group", "topic", "env", "version"},
		"kafka_consumer_group_topic_sum_lag_in_offsets": {"clustername", "group", "topic", "env", "version"},
		"kafka_consumer_group_topic_sum_lag_in_seconds": {"clustername", "group", "topic", "env", "version"},
		"kafka_total_groups_checked":                    {"clustername", "env", "version"},
		"kafka_iteration_time_seconds":                  {},
	}

	// Check that all expected metrics are registered with the correct labels
	for _, mf := range metricFamilies {
		if expectedLabels, found := expectedMetrics[*mf.Name]; found {
			for _, metric := range mf.Metric {
				// For each metric, check that it has the expected labels
				labelsFound := make(map[string]bool)
				for _, label := range metric.Label {
					labelsFound[*label.Name] = true
				}

				// Ensure all expected labels are present
				for _, expectedLabel := range expectedLabels {
					assert.True(t, labelsFound[expectedLabel], "Expected label not found: "+expectedLabel+" for metric: "+*mf.Name)
				}
			}
		} else {
			t.Errorf("Unexpected metric found: %s", *mf.Name)
		}
	}

	// Ensure all expected metrics were gathered
	for expectedMetric := range expectedMetrics {
		found := false
		for _, mf := range metricFamilies {
			if *mf.Name == expectedMetric {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected metric not found: "+expectedMetric)
	}
}

func TestProcessMetrics_NoLag(t *testing.T) {
	// Create a new registry for the test
	registry := prometheus.NewRegistry()
	pm := setupPrometheusMetricsWithRegistry(registry)
	startTime := time.Now()

	metricsToExportChan := make(chan *structs.Group, 1)

	// Create a test group with no lag
	group := &structs.Group{
		Name:        "test-group",
		ClusterName: "test-cluster",
		Topics: []structs.Topic{
			{
				Name: "test-topic",
				Partitions: []structs.Partition{
					{Number: 0, LagInOffsets: 0, LagInSeconds: 0},
				},
				MaxLagInOffsets: 0,
				MaxLagInSeconds: 0,
				SumLagInOffsets: 0,
				SumLagInSeconds: 0,
			},
		},
		MaxLagInOffsets: 0,
		MaxLagInSeconds: 0,
		SumLagInOffsets: 0,
		SumLagInSeconds: 0,
	}

	metricsToExportChan <- group
	close(metricsToExportChan)

	// Process the metrics
	pm.ProcessMetrics(metricsToExportChan, 1, startTime)

	// Validate that metrics are set to 0 for this group
	expectedPartitionLabels := prometheus.Labels{
		"clustername": "test-cluster",
		"group":       "test-group",
		"topic":       "test-topic",
		"partition":   "0",
		"env":         "test",
		"version":     "v1",
	}

	assert.Equal(t, 0.0, testutil.ToFloat64(pm.lagInOffsets.With(expectedPartitionLabels)))
	assert.Equal(t, 0.0, testutil.ToFloat64(pm.lagInSeconds.With(expectedPartitionLabels)))
}

func TestPrometheusMetrics_MultipleClusters(t *testing.T) {
	registry := prometheus.NewRegistry()

	// Setup PrometheusMetrics and register metrics with the registry
	pm := setupPrometheusMetricsWithRegistry(registry)

	// Prepare mock data for Kafka consumer groups
	metricsToExportChan := make(chan *structs.Group, 3)

	// Create test groups from different clusters
	groupCluster1 := &structs.Group{
		Name:        "group1",
		ClusterName: "cluster1",
		Topics: []structs.Topic{
			{
				Name: "topic1",
				Partitions: []structs.Partition{
					{Number: 0, LagInOffsets: 100, LagInSeconds: 200},
				},
				MaxLagInOffsets: 100,
				MaxLagInSeconds: 200,
				SumLagInOffsets: 100,
				SumLagInSeconds: 200,
			},
		},
		MaxLagInOffsets: 100,
		MaxLagInSeconds: 200,
		SumLagInOffsets: 100,
		SumLagInSeconds: 200,
	}

	groupCluster2 := &structs.Group{
		Name:        "group2",
		ClusterName: "cluster2",
		Topics: []structs.Topic{
			{
				Name: "topic2",
				Partitions: []structs.Partition{
					{Number: 0, LagInOffsets: 150, LagInSeconds: 300},
				},
				MaxLagInOffsets: 150,
				MaxLagInSeconds: 300,
				SumLagInOffsets: 150,
				SumLagInSeconds: 300,
			},
		},
		MaxLagInOffsets: 150,
		MaxLagInSeconds: 300,
		SumLagInOffsets: 150,
		SumLagInSeconds: 300,
	}

	groupCluster1Second := &structs.Group{
		Name:        "group3",
		ClusterName: "cluster1", // Same cluster as groupCluster1
		Topics: []structs.Topic{
			{
				Name: "topic3",
				Partitions: []structs.Partition{
					{Number: 0, LagInOffsets: 50, LagInSeconds: 100},
				},
				MaxLagInOffsets: 50,
				MaxLagInSeconds: 100,
				SumLagInOffsets: 50,
				SumLagInSeconds: 100,
			},
		},
		MaxLagInOffsets: 50,
		MaxLagInSeconds: 100,
		SumLagInOffsets: 50,
		SumLagInSeconds: 100,
	}

	// Send the test groups into the channel and close it
	metricsToExportChan <- groupCluster1
	metricsToExportChan <- groupCluster2
	metricsToExportChan <- groupCluster1Second
	close(metricsToExportChan)

	// Process the metrics
	startTime := time.Now()
	pm.ProcessMetrics(metricsToExportChan, 1, startTime)

	// Check total groups checked per cluster
	metricFamilies, err := registry.Gather()
	assert.NoError(t, err)

	expectedTotalGroupsChecked := map[string]float64{
		"cluster1": 2.0, // groupCluster1 and groupCluster1Second
		"cluster2": 1.0, // groupCluster2
	}

	for _, mf := range metricFamilies {
		if *mf.Name == "kafka_total_groups_checked" {
			for _, metric := range mf.Metric {
				clusterName := ""
				for _, label := range metric.Label {
					if *label.Name == "clustername" {
						clusterName = *label.Value
					}
				}

				if expectedValue, found := expectedTotalGroupsChecked[clusterName]; found {
					assert.Equal(t, expectedValue, *metric.Gauge.Value, "Unexpected total groups checked for cluster "+clusterName)
				} else {
					t.Errorf("Unexpected cluster found: %s", clusterName)
				}
			}
		}
	}
}

func TestPrometheusMetrics_NoTopics(t *testing.T) {
	registry := prometheus.NewRegistry()

	// Setup PrometheusMetrics and register metrics with the registry
	pm := setupPrometheusMetricsWithRegistry(registry)

	// Prepare mock data for Kafka consumer groups
	metricsToExportChan := make(chan *structs.Group, 1)

	// Create a test group with no topics
	group := &structs.Group{
		Name:        "test-group",
		ClusterName: "test-cluster",
		Topics:      []structs.Topic{}, // No topics
	}

	metricsToExportChan <- group
	close(metricsToExportChan)

	// Process the metrics
	startTime := time.Now()
	pm.ProcessMetrics(metricsToExportChan, 1, startTime)

	// Gather and validate metrics
	metricFamilies, err := registry.Gather()
	assert.NoError(t, err)

	// Ensure total groups checked is recorded even if there are no topics
	for _, mf := range metricFamilies {
		if *mf.Name == "kafka_total_groups_checked" {
			for _, metric := range mf.Metric {
				assert.Equal(t, 1.0, *metric.Gauge.Value, "Expected 1 group checked even with no topics")
			}
		}
	}
}

func TestPrometheusMetrics_Concurrency(t *testing.T) {
	registry := prometheus.NewRegistry()

	// Setup PrometheusMetrics and register metrics with the registry
	pm := setupPrometheusMetricsWithRegistry(registry)

	// Prepare mock data for Kafka consumer groups
	metricsToExportChan := make(chan *structs.Group, 10)

	// Create 10 test groups
	for i := 0; i < 10; i++ {
		group := &structs.Group{
			Name:        "group" + strconv.Itoa(i),
			ClusterName: "cluster" + strconv.Itoa(i%2), // Alternating between cluster0 and cluster1
			Topics: []structs.Topic{
				{
					Name: "topic" + strconv.Itoa(i),
					Partitions: []structs.Partition{
						{Number: 0, LagInOffsets: int64(i * 100), LagInSeconds: int64(i * 200)},
					},
					MaxLagInOffsets: int64(i * 100),
					MaxLagInSeconds: int64(i * 200),
					SumLagInOffsets: int64(i * 100),
					SumLagInSeconds: int64(i * 200),
				},
			},
			MaxLagInOffsets: int64(i * 100),
			MaxLagInSeconds: int64(i * 200),
			SumLagInOffsets: int64(i * 100),
			SumLagInSeconds: int64(i * 200),
		}
		metricsToExportChan <- group
	}

	close(metricsToExportChan)

	// Process the metrics with 5 workers
	startTime := time.Now()
	pm.ProcessMetrics(metricsToExportChan, 5, startTime)

	// Gather and validate metrics
	metricFamilies, err := registry.Gather()
	assert.NoError(t, err)

	// Ensure groups from both clusters are correctly processed and counted
	for _, mf := range metricFamilies {
		if *mf.Name == "kafka_total_groups_checked" {
			for _, metric := range mf.Metric {
				clusterName := ""
				for _, label := range metric.Label {
					if *label.Name == "clustername" {
						clusterName = *label.Value
					}
				}
				if clusterName == "cluster0" {
					assert.Equal(t, 5.0, *metric.Gauge.Value, "Expected 5 groups for cluster0")
				} else if clusterName == "cluster1" {
					assert.Equal(t, 5.0, *metric.Gauge.Value, "Expected 5 groups for cluster1")
				}
			}
		}
	}
}
