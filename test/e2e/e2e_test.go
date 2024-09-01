package e2e

import (
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

func TestHealthEndpoint(t *testing.T) {
	// Wait for the application to be up and running
	time.Sleep(5 * time.Second)

	resp, err := http.Get("http://localhost:8080/healthz")
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestMetricsEndpoint(t *testing.T) {
	// Wait for the application to be up and running
	time.Sleep(5 * time.Second)

	resp, err := http.Get("http://localhost:8080/metrics")
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	time.Sleep(17 * time.Second)
}
