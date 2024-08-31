package metrics

import (
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
	registry.MustRegister(pm.totalGroupsChecked)
	registry.MustRegister(pm.iterationTimeSeconds)

	return pm
}

func TestPrometheusMetrics_ProcessMetrics(t *testing.T) {
	registry := prometheus.NewRegistry() // Create a new registry for the test
	pm := setupPrometheusMetricsWithRegistry(registry)

	metricsToExportChan := make(chan *structs.Group, 1)
	startTime := time.Now()

	// Create a test group with some metrics
	group := &structs.Group{
		Name: "test-group",
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

	metricsToExportChan <- group
	close(metricsToExportChan)

	// Process the metrics
	pm.ProcessMetrics(metricsToExportChan, 1, startTime)

	// Validate that metrics are recorded using the custom registry
	expectedLabels := prometheus.Labels{
		"group":     "test-group",
		"topic":     "test-topic",
		"partition": "0",
		"env":       "test",
		"version":   "v1",
	}

	assert.Equal(t, 100.0, testutil.ToFloat64(pm.lagInOffsets.With(expectedLabels)))
	assert.Equal(t, 200.0, testutil.ToFloat64(pm.lagInSeconds.With(expectedLabels)))

	assert.Equal(t, 100.0, testutil.ToFloat64(pm.groupMaxLagInOffsets.With(prometheus.Labels{"group": "test-group", "env": "test", "version": "v1"})))
	assert.Equal(t, 200.0, testutil.ToFloat64(pm.groupMaxLagInSeconds.With(prometheus.Labels{"group": "test-group", "env": "test", "version": "v1"})))
	assert.Equal(t, 100.0, testutil.ToFloat64(pm.groupSumLagInOffsets.With(prometheus.Labels{"group": "test-group", "env": "test", "version": "v1"})))
	assert.Equal(t, 200.0, testutil.ToFloat64(pm.groupSumLagInSeconds.With(prometheus.Labels{"group": "test-group", "env": "test", "version": "v1"})))

	// Check total groups checked
	assert.Equal(t, 1.0, testutil.ToFloat64(pm.totalGroupsChecked))
}

func TestIterationTimeCalculation(t *testing.T) {
	registry := prometheus.NewRegistry() // Create a new registry for the test
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
	assert.Greater(t, iterationTime, 0.1)
	assert.Less(t, iterationTime, 0.15)
}
