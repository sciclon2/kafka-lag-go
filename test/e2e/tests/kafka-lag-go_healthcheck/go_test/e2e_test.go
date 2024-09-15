package e2e

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/sciclon2/kafka-lag-go/pkg/structs"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	if os.Getenv("RUN_E2E_TESTS") != "true" {
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestHealthEndpoint(t *testing.T) {
	// Wait for the application to be up and running (simulate delay if necessary)
	time.Sleep(5 * time.Second)

	// Make the HTTP request to the health check endpoint
	resp, err := http.Get("http://localhost:8080/healthz")
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Parse the response body
	defer resp.Body.Close()
	var healthStatus structs.HealthStatus
	err = json.NewDecoder(resp.Body).Decode(&healthStatus)
	assert.NoError(t, err)

	// Check that the status is either "OK" or "Unhealthy"
	assert.Condition(t, func() bool {
		return healthStatus.Status == "OK" || healthStatus.Status == "Unhealthy"
	}, "Expected status to be either 'OK' or 'Unhealthy'")

}
