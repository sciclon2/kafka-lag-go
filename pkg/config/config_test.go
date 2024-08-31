package config

import (
	"io/ioutil"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// Helper function to create a temporary YAML file for testing
func createTempConfigFile(t *testing.T, content string) string {
	tmpFile, err := ioutil.TempFile("", "config-*.yaml")
	assert.NoError(t, err)
	_, err = tmpFile.Write([]byte(content))
	assert.NoError(t, err)
	tmpFile.Close()
	t.Cleanup(func() {
		os.Remove(tmpFile.Name())
	})
	return tmpFile.Name()
}

// Helper function to create a valid Config object
func createValidConfig() *Config {
	return &Config{
		App: struct {
			ClusterName       string `yaml:"cluster_name"`
			IterationInterval string `yaml:"iteration_interval"`
			NumWorkers        int    `yaml:"num_workers"`
			LogLevel          string `yaml:"log_level"`
			HealthCheckPort   int    `yaml:"health_check_port"`
			HealthCheckPath   string `yaml:"health_check_path"`
		}{
			ClusterName:       "test-cluster",
			IterationInterval: "20s",
			NumWorkers:        4,
			LogLevel:          "info",
			HealthCheckPort:   8080,
			HealthCheckPath:   "/healthz",
		},
		Storage: struct {
			Type  string `yaml:"type"`
			Redis struct {
				Address              string `yaml:"address"`
				Port                 int    `yaml:"port"`
				ClientRequestTimeout string `yaml:"client_request_timeout"`
				ClientIdleTimeout    string `yaml:"client_idle_timeout"`
				RetentionTTLSeconds  int    `yaml:"retention_ttl_seconds"`
			} `yaml:"redis"`
		}{
			Type: "redis",
			Redis: struct {
				Address              string `yaml:"address"`
				Port                 int    `yaml:"port"`
				ClientRequestTimeout string `yaml:"client_request_timeout"`
				ClientIdleTimeout    string `yaml:"client_idle_timeout"`
				RetentionTTLSeconds  int    `yaml:"retention_ttl_seconds"`
			}{
				Address:              "localhost",
				Port:                 6379,
				ClientRequestTimeout: "60s",
				ClientIdleTimeout:    "5m",
				RetentionTTLSeconds:  7200,
			},
		},
		Kafka: struct {
			Brokers              []string `yaml:"brokers"`
			ClientRequestTimeout string   `yaml:"client_request_timeout"`
			MetadataFetchTimeout string   `yaml:"metadata_fetch_timeout"`
			ConsumerGroups       struct {
				Whitelist *regexp.Regexp `yaml:"whitelist"`
				Blacklist *regexp.Regexp `yaml:"blacklist"`
			} `yaml:"consumer_groups"`
			SSL struct {
				Enabled               bool   `yaml:"enabled"`
				ClientCertificateFile string `yaml:"client_certificate_file"`
				ClientKeyFile         string `yaml:"client_key_file"`
				InsecureSkipVerify    bool   `yaml:"insecure_skip_verify"`
			} `yaml:"ssl"`
			SASL struct {
				Enabled   bool   `yaml:"enabled"`
				Mechanism string `yaml:"mechanism"`
				User      string `yaml:"user"`
				Password  string `yaml:"password"`
			} `yaml:"sasl"`
		}{
			Brokers:              []string{"localhost:9092"},
			ClientRequestTimeout: "30s",
			MetadataFetchTimeout: "5s",
			ConsumerGroups: struct {
				Whitelist *regexp.Regexp `yaml:"whitelist"`
				Blacklist *regexp.Regexp `yaml:"blacklist"`
			}{
				Whitelist: regexp.MustCompile(".*"),
				Blacklist: regexp.MustCompile(""),
			},
			SSL: struct {
				Enabled               bool   `yaml:"enabled"`
				ClientCertificateFile string `yaml:"client_certificate_file"`
				ClientKeyFile         string `yaml:"client_key_file"`
				InsecureSkipVerify    bool   `yaml:"insecure_skip_verify"`
			}{
				Enabled:               true,
				ClientCertificateFile: "/path/to/cert.pem",
				ClientKeyFile:         "/path/to/key.pem",
				InsecureSkipVerify:    true,
			},
			SASL: struct {
				Enabled   bool   `yaml:"enabled"`
				Mechanism string `yaml:"mechanism"`
				User      string `yaml:"user"`
				Password  string `yaml:"password"`
			}{
				Enabled:   true,
				Mechanism: "SCRAM-SHA-256",
				User:      "user",
				Password:  "password",
			},
		},
	}
}

func TestLoadConfig(t *testing.T) {
	// Test valid config loading
	configYAML := `
app:
  cluster_name: "test-cluster"
  iteration_interval: "20s"
kafka:
  brokers: ["localhost:9092"]
  consumer_groups:
    whitelist: ".*"
  ssl:
    enabled: true
    client_certificate_file: "/path/to/cert.pem"
    client_key_file: "/path/to/key.pem"
  sasl:
    enabled: true
    mechanism: "SCRAM-SHA-256"
    user: "user"
    password: "password"
storage:
  redis:
    address: "localhost"
    port: 6379
`
	tmpFileName := createTempConfigFile(t, configYAML)
	config, err := LoadConfig(tmpFileName)
	assert.NoError(t, err)
	assert.Equal(t, "test-cluster", config.App.ClusterName)
	assert.Equal(t, "20s", config.App.IterationInterval)
	assert.Equal(t, "localhost:9092", config.Kafka.Brokers[0])
	assert.True(t, config.Kafka.SSL.Enabled)
	assert.Equal(t, "localhost", config.Storage.Redis.Address)
	assert.Equal(t, 6379, config.Storage.Redis.Port)
}

func TestLoadConfigDefaults(t *testing.T) {
	// Test loading with defaults
	configYAML := `
app:
  cluster_name: "test-cluster"
`
	tmpFileName := createTempConfigFile(t, configYAML)
	config, err := LoadConfig(tmpFileName)
	assert.NoError(t, err)
	assert.Equal(t, "test-cluster", config.App.ClusterName)
	assert.Equal(t, "30s", config.App.IterationInterval)
	assert.Equal(t, "localhost", config.Storage.Redis.Address)
	assert.Equal(t, 6379, config.Storage.Redis.Port)
	assert.Equal(t, 7200, config.Storage.Redis.RetentionTTLSeconds)
	assert.Equal(t, "60s", config.Storage.Redis.ClientRequestTimeout)
	assert.Equal(t, "5m", config.Storage.Redis.ClientIdleTimeout)
}

func TestValidateConfig(t *testing.T) {
	// Test valid config
	validConfig := createValidConfig()
	err := validateConfig(validConfig)
	assert.NoError(t, err)

	// Test invalid config scenarios
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string
	}{
		{
			name: "missing cluster name",
			modify: func(c *Config) {
				c.App.ClusterName = ""
			},
			wantErr: "cluster_name is required and cannot be empty",
		},
		{
			name: "missing Redis client request timeout",
			modify: func(c *Config) {
				c.Storage.Redis.ClientRequestTimeout = ""
			},
			wantErr: "redis client request timeout is missing",
		},
		{
			name: "SASL enabled but missing user",
			modify: func(c *Config) {
				c.Kafka.SASL.User = ""
			},
			wantErr: "sasl configuration is enabled but missing user or password",
		},
		{
			name: "iteration interval too short",
			modify: func(c *Config) {
				c.App.IterationInterval = "10s"
			},
			wantErr: "iteration_interval must be at least 15 seconds",
		},
		{
			name: "invalid Redis port",
			modify: func(c *Config) {
				c.Storage.Redis.Port = 70000
			},
			wantErr: "redis port must be between 1 and 65535",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createValidConfig()
			tt.modify(config)
			err := validateConfig(config)
			assert.Error(t, err)
			assert.Equal(t, tt.wantErr, err.Error())
		})
	}
}

func TestGetRedisAddress(t *testing.T) {
	config := createValidConfig()
	assert.Equal(t, "localhost:6379", config.GetRedisAddress())
}

func TestGetIterationInterval(t *testing.T) {
	config := createValidConfig()
	config.App.IterationInterval = "45s"
	duration, err := config.GetIterationInterval()
	assert.NoError(t, err)
	assert.Equal(t, 45*time.Second, duration)
}

func TestSetLogLevel(t *testing.T) {
	config := &Config{
		App: struct {
			ClusterName       string `yaml:"cluster_name"`
			IterationInterval string `yaml:"iteration_interval"`
			NumWorkers        int    `yaml:"num_workers"`
			LogLevel          string `yaml:"log_level"`
			HealthCheckPort   int    `yaml:"health_check_port"`
			HealthCheckPath   string `yaml:"health_check_path"`
		}{
			LogLevel: "debug",
		},
	}

	config.SetLogLevel()
	assert.Equal(t, logrus.DebugLevel, logrus.GetLevel())

	// Test invalid log level
	config.App.LogLevel = "invalid"
	config.SetLogLevel()
	assert.Equal(t, logrus.InfoLevel, logrus.GetLevel()) // Should default to info
}
