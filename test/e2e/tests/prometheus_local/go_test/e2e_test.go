package e2e

import (
	"bufio"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	if os.Getenv("RUN_E2E_TESTS") != "true" {
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type PrometheusQueryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// TestMetricExistsAndHasValue checks that the specified metric exists in /metrics and has a valid value
func TestQueryKafkaConsumerMetricLocal(t *testing.T) {
	// Wait for the application to be up and running
	time.Sleep(3 * time.Second)

	// Make a request to the /metrics endpoint
	metricsURL := "http://localhost:9099/metrics" // Update the port if necessary
	resp, err := http.Get(metricsURL)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	defer resp.Body.Close()

	// Scan through the response body line by line
	scanner := bufio.NewScanner(resp.Body)
	expectedMetric := "kafka_consumer_group_topic_max_lag_in_offsets"
	metricFound := false

	for scanner.Scan() {
		line := scanner.Text()

		// Skip comments or empty lines
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}

		// Check if the line starts with the expected metric
		if strings.HasPrefix(line, expectedMetric) {
			metricFound = true

			// Split the line into the metric and its value
			parts := strings.Fields(line)
			assert.GreaterOrEqual(t, len(parts), 2, "Expected metric line to have at least two parts: metric and value")

			// Parse the value and ensure it's a valid number (non-zero)
			metricValue, err := strconv.ParseFloat(parts[len(parts)-1], 64)
			assert.NoError(t, err, "Expected metric value to be a valid float")
			assert.GreaterOrEqual(t, metricValue, 0.0, "Expected metric value to be non-negative")
			break
		}
	}

	// Ensure the metric was found
	assert.True(t, metricFound, "The metric '%s' was not found in the /metrics endpoint", expectedMetric)
}
