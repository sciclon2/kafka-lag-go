package metrics

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sciclon2/kafka-lag-go/pkg/config"
	"github.com/sirupsen/logrus"

	"github.com/prometheus/prometheus/prompb"
)

func (pm *PrometheusMetrics) StartRemoteWriteExporter(cfg *config.Config) {
	if !cfg.PrometheusRemoteWrite.Enabled {
		return
	}

	// Parse the iteration interval
	//iterationInterval, _ := time.ParseDuration(cfg.App.IterationInterval)
	iterationInterval, _ := time.ParseDuration("5s")
	logrus.Infof("Starting Prometheus remote writer with iteration interval: %s", iterationInterval)
	go func() {
		ticker := time.NewTicker(iterationInterval)
		defer ticker.Stop()

		for range ticker.C {
			err := pm.RemoteWriteSend(&cfg.PrometheusRemoteWrite)
			if err != nil {
				logrus.Errorf("Failed to export metrics to remote write: %v", err)
			}
		}
	}()
}

// SendRemoteWrite exports metrics to a remote Prometheus instance
// Entry point for sending remote write metrics
func (pm *PrometheusMetrics) RemoteWriteSend(cfg *config.PrometheusRemoteWriteConfig) error {
	// Setup TLS if necessary
	tlsConfig, err := pm.RemoteWriteSetupTLS(cfg)
	if err != nil {
		return err
	}

	// Gather and convert Prometheus metrics
	timeSeries, err := pm.RemoteWriteGatherMetrics()
	if err != nil {
		return err
	}

	// Prepare and send the HTTP request with the gathered metrics
	return pm.RemoteWriteSendRequest(cfg, tlsConfig, timeSeries)
}

// RemoteWriteSetupTLS configures TLS if enabled
func (pm *PrometheusMetrics) RemoteWriteSetupTLS(cfg *config.PrometheusRemoteWriteConfig) (*tls.Config, error) {
	if !cfg.TLSConfig.Enabled {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.TLSConfig.InsecureSkipVerify,
	}

	// Load client certificate if provided
	if cfg.TLSConfig.CertFile != "" && cfg.TLSConfig.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSConfig.CertFile, cfg.TLSConfig.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %v", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Load CA certificate if provided
	if cfg.TLSConfig.CACertFile != "" {
		caCert, err := ioutil.ReadFile(cfg.TLSConfig.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %v", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to add CA certificate to pool")
		}
		tlsConfig.RootCAs = caCertPool
	}

	return tlsConfig, nil
}

// RemoteWriteGatherMetrics collects Prometheus metrics and converts them to TimeSeries format
func (pm *PrometheusMetrics) RemoteWriteGatherMetrics() ([]prompb.TimeSeries, error) {
	var timeSeries []prompb.TimeSeries

	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return nil, fmt.Errorf("failed to gather metrics: %w", err)
	}

	for _, metricFamily := range metricFamilies {
		for _, metric := range metricFamily.Metric {
			labels := []prompb.Label{
				{
					Name:  "__name__",             // Metric name
					Value: metricFamily.GetName(), // Preserve original name
				},
			}

			// Preserve the original labels
			for _, label := range metric.GetLabel() {
				labels = append(labels, prompb.Label{
					Name:  label.GetName(),
					Value: label.GetValue(),
				})
			}

			// Use the timestamp from the metric, if it exists
			timestamp := metric.GetTimestampMs()
			if timestamp == 0 {
				// If no timestamp is provided, use the current timestamp
				timestamp = time.Now().Unix() * 1000
			}
			// Append each sample (value and timestamp)
			timeSeries = append(timeSeries, prompb.TimeSeries{
				Labels: labels,
				Samples: []prompb.Sample{
					{
						Value:     metric.GetGauge().GetValue(), // Get the actual metric value
						Timestamp: timestamp,
					},
				},
			})
		}
	}

	return timeSeries, nil
}

// RemoteWriteSendRequest sends the collected metrics via HTTP to the remote endpoint
func (pm *PrometheusMetrics) RemoteWriteSendRequest(cfg *config.PrometheusRemoteWriteConfig, tlsConfig *tls.Config, timeSeries []prompb.TimeSeries) error {
	// Prepare WriteRequest with the collected time-series data
	writeRequest := &prompb.WriteRequest{
		Timeseries: timeSeries,
	}

	// Serialize the write request
	data, err := writeRequest.Marshal()
	if err != nil {
		return fmt.Errorf("prometheus remote write failed to marshal write request: %w", err)
	}

	// Compress the data using Snappy
	compressedData := snappy.Encode(nil, data)

	// Create an HTTP request
	req, err := http.NewRequest("POST", cfg.URL, bytes.NewReader(compressedData))
	if err != nil {
		return fmt.Errorf("prometheus remote write failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")

	// Authentication (Basic Auth or Bearer Token) - Make sure both are optional
	if cfg.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.BearerToken)
	} else if cfg.BasicAuth.Username != "" && cfg.BasicAuth.Password != "" {
		req.SetBasicAuth(cfg.BasicAuth.Username, cfg.BasicAuth.Password)
	}

	// Execute the request
	timeoutDuration, err := time.ParseDuration(cfg.Timeout)
	if err != nil {
		return fmt.Errorf("prometheus remote write invalid timeout duration: %w", err)
	}

	client := &http.Client{
		Timeout:   time.Duration(timeoutDuration),
		Transport: &http.Transport{TLSClientConfig: tlsConfig},
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("prometheus remote write failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Allow 200 OK and 204 No Content as valid responses
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("prometheus remote write received non-OK status code: %d", resp.StatusCode)
	}

	logrus.Debugf("prometheus remote write Metrics successfully sent to %s", cfg.URL)
	return nil
}
