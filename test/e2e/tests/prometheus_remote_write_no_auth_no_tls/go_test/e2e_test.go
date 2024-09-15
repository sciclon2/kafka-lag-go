package e2e

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
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

// it makes sure the metrics reach the final destination of prometheus remote write
func TestQueryKafkaConsumerMetric(t *testing.T) {
	// Wait for the application to be up and running
	time.Sleep(3 * time.Second)

	// Query Prometheus for the metric "kafka_consumer_group_topic_max_lag_in_offsets"
	queryURL := "http://localhost:9090/api/v1/query?query=kafka_consumer_group_topic_max_lag_in_offsets"
	resp, err := http.Get(queryURL)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)

	var prometheusResponse PrometheusQueryResponse
	err = json.Unmarshal(body, &prometheusResponse)
	assert.NoError(t, err)

	// Check that the response status is "success"
	assert.Equal(t, "success", prometheusResponse.Status)

	// Ensure that there are results
	assert.Greater(t, len(prometheusResponse.Data.Result), 0, "Expected to find the 'kafka_consumer_group_topic_max_lag_in_offsets' metric")

	// Check if the result contains the metric with the expected name
	found := false
	for _, result := range prometheusResponse.Data.Result {
		metricName := result.Metric["__name__"]
		if metricName == "kafka_consumer_group_topic_max_lag_in_offsets" {
			found = true
			break
		}
	}
	assert.True(t, found, "The 'kafka_consumer_group_topic_max_lag_in_offsets' metric was not found in the Prometheus results")
}
