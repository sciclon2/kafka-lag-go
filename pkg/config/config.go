package config

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"gopkg.in/yaml.v2"
)

// Config represents the application configuration
type Config struct {
	PrometheusLocal       PrometheusLocalConfig       `yaml:"prometheus_local"`
	PrometheusRemoteWrite PrometheusRemoteWriteConfig `yaml:"prometheus_remote_write"`
	KafkaClusters         []KafkaCluster              `yaml:"kafka_clusters"`
	Storage               struct {
		Type  string      `yaml:"type"` // e.g., "redis", "mysql" (for future use)
		Redis RedisConfig `yaml:"redis"`
	} `yaml:"storage"`
	App struct {
		IterationInterval string `yaml:"iteration_interval"`
		NumWorkers        int    `yaml:"num_workers"`
		LogLevel          string `yaml:"log_level"`
		HealthCheckPort   int    `yaml:"health_check_port"`
		HealthCheckPath   string `yaml:"health_check_path"`
	} `yaml:"app"`
}

// RedisConfig holds Redis-related configurations
type RedisConfig struct {
	Address              string     `yaml:"address"`
	Port                 int        `yaml:"port"`
	ClientRequestTimeout string     `yaml:"client_request_timeout"`
	ClientIdleTimeout    string     `yaml:"client_idle_timeout"`
	RetentionTTLSeconds  int        `yaml:"retention_ttl_seconds"`
	Auth                 AuthConfig `yaml:"auth"`
	SSL                  SSLConfig  `yaml:"ssl"`
}

// AuthConfig holds authentication-related settings (for Redis ACL on Redis)
type AuthConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Username string `yaml:"username,omitempty"` // Redis ACL username (optional)
	Password string `yaml:"password,omitempty"` // Redis ACL password (optional)
}

// KafkaCluster represents the configuration for a single Kafka cluster
type KafkaCluster struct {
	Name                 string         `yaml:"name"`
	Brokers              []string       `yaml:"brokers"`
	ClientRequestTimeout string         `yaml:"client_request_timeout"`
	MetadataFetchTimeout string         `yaml:"metadata_fetch_timeout"`
	ConsumerGroups       ConsumerGroups `yaml:"consumer_groups"`
	SSL                  SSLConfig      `yaml:"ssl"`
	SASL                 SASLConfig     `yaml:"sasl"`
}

// ConsumerGroups represents the consumer group settings
type ConsumerGroups struct {
	Whitelist *regexp.Regexp `yaml:"whitelist"`
	Blacklist *regexp.Regexp `yaml:"blacklist"`
}

// SSLConfig represents the SSL settings for Kafka
type SSLConfig struct {
	Enabled               bool   `yaml:"enabled"`
	ClientCertificateFile string `yaml:"client_certificate_file"`
	ClientKeyFile         string `yaml:"client_key_file"`
	InsecureSkipVerify    bool   `yaml:"insecure_skip_verify"`
	CACertFile            string `yaml:"ca_cert_file,omitempty"`
}

// SASLConfig represents the SASL settings for Kafka
type SASLConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Mechanism string `yaml:"mechanism"`
	User      string `yaml:"user"`
	Password  string `yaml:"password"`
}

type PrometheusLocalConfig struct {
	MetricsPort int               `yaml:"metrics_port"`
	Labels      map[string]string `yaml:"labels,omitempty"`
}

type PrometheusRemoteWriteConfig struct {
	Enabled     bool              `yaml:"enabled"`
	URL         string            `yaml:"url"`
	Headers     map[string]string `yaml:"headers,omitempty"`      // Optional headers like Authorization
	Timeout     string            `yaml:"timeout"`                // Timeout duration for pushing metrics
	BasicAuth   BasicAuthConfig   `yaml:"basic_auth,omitempty"`   // For basic auth
	BearerToken string            `yaml:"bearer_token,omitempty"` // Bearer token for authentication
	TLSConfig   TLSConfig         `yaml:"tls_config,omitempty"`   // Optional TLS configuration
}

type BasicAuthConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}
type TLSConfig struct {
	Enabled            bool   `yaml:"enabled"` // Optional flag to enable/disable TLS
	CertFile           string `yaml:"cert_file,omitempty"`
	KeyFile            string `yaml:"key_file,omitempty"`
	CACertFile         string `yaml:"ca_cert_file,omitempty"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify,omitempty"`
}

func GetConfigFilePath() string {
	// Define a command-line flag for the configuration file path, with no default value.
	configPath := flag.String("config-file", "", "Path to the configuration file")
	flag.Parse()

	// Check if the configPath is provided either via the command-line flag or the environment variable.
	if *configPath == "" {
		*configPath = os.Getenv("CONFIG_FILE")
	}

	// If neither is provided, exit with an error.
	if *configPath == "" {
		log.Fatalf("--config-file flag or CONFIG_FILE environment variable is required. Please specify the path to the configuration file.")
	}

	// Check if the file exists before trying to load it.
	if _, err := os.Stat(*configPath); os.IsNotExist(err) {
		log.Fatalf("Configuration file not found: %s", *configPath)
	}

	return *configPath
}

// LoadConfig loads configuration from a YAML file and validates it
func LoadConfig(filePath string) (*Config, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	// Set default values if not provided
	setDefaults(&config)

	// Compile regex patterns and validate the configuration
	err = compileRegexPatterns(&config)
	if err != nil {
		return nil, err
	}

	// Validate the configuration
	err = validateConfig(&config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func compileRegexPatterns(config *Config) error {
	var err error

	// Iterate over each Kafka cluster
	for _, cluster := range config.KafkaClusters {
		// Compile whitelist pattern if it is not nil
		if cluster.ConsumerGroups.Whitelist != nil {
			cluster.ConsumerGroups.Whitelist, err = regexp.Compile(cluster.ConsumerGroups.Whitelist.String())
			if err != nil {
				return fmt.Errorf("kafka cluster '%s' has an invalid consumer group whitelist regex pattern: %w", cluster.Name, err)
			}
		}

		// Compile blacklist pattern if it is not nil
		if cluster.ConsumerGroups.Blacklist != nil {
			cluster.ConsumerGroups.Blacklist, err = regexp.Compile(cluster.ConsumerGroups.Blacklist.String())
			if err != nil {
				return fmt.Errorf("kafka cluster '%s' has an invalid consumer group blacklist regex pattern: %w", cluster.Name, err)
			}
		}
	}

	return nil
}

// setDefaults sets default values for the configuration if not provided
func setDefaults(config *Config) {
	if config.Storage.Type == "" {
		config.Storage.Type = "redis"
	}
	if config.Storage.Redis.Address == "" {
		config.Storage.Redis.Address = "localhost"
	}
	if config.Storage.Redis.Port == 0 {
		config.Storage.Redis.Port = 6379
	}
	if config.Storage.Redis.RetentionTTLSeconds == 0 {
		config.Storage.Redis.RetentionTTLSeconds = 7200 // Default to 2 hours
	}
	if config.Storage.Redis.ClientRequestTimeout == "" {
		config.Storage.Redis.ClientRequestTimeout = "60s"
	}
	if config.Storage.Redis.ClientIdleTimeout == "" {
		config.Storage.Redis.ClientIdleTimeout = "5m"
	}
	if !config.Storage.Redis.Auth.Enabled {
		config.Storage.Redis.Auth.Enabled = false
	}
	if !config.Storage.Redis.SSL.Enabled {
		config.Storage.Redis.SSL.Enabled = false
	}
	if config.Storage.Redis.SSL.Enabled && !config.Storage.Redis.SSL.InsecureSkipVerify {
		config.Storage.Redis.SSL.InsecureSkipVerify = true
	}
	if config.App.IterationInterval == "" {
		config.App.IterationInterval = "30s"
	}
	if config.App.NumWorkers == 0 {
		config.App.NumWorkers = runtime.NumCPU()
	}
	if config.App.NumWorkers == 0 {
		config.App.NumWorkers = runtime.NumCPU() // Default to the number of CPUs if not set
	}
	if config.App.LogLevel == "" {
		config.App.LogLevel = "info"
	}
	if config.App.HealthCheckPort == 0 {
		config.App.HealthCheckPort = 8080
	}
	if config.App.HealthCheckPath == "" {
		config.App.HealthCheckPath = "/healthz"
	}
	// Set defaults for each Kafka cluster
	for i := range config.KafkaClusters {
		cluster := &config.KafkaClusters[i]
		if cluster.ClientRequestTimeout == "" {
			cluster.ClientRequestTimeout = "30s"
		}
		if cluster.MetadataFetchTimeout == "" {
			cluster.MetadataFetchTimeout = "5s"
		}
		// Handle consumer group regex patterns
		if cluster.ConsumerGroups.Whitelist != nil && cluster.ConsumerGroups.Whitelist.String() == "" {
			cluster.ConsumerGroups.Whitelist = nil
		}
		if cluster.ConsumerGroups.Blacklist != nil && cluster.ConsumerGroups.Blacklist.String() == "" {
			cluster.ConsumerGroups.Blacklist = nil
		}
		// Ensure SSL is enabled before considering InsecureSkipVerify
		if cluster.SSL.Enabled && !cluster.SSL.InsecureSkipVerify {
			cluster.SSL.InsecureSkipVerify = true
		}
	}
	// Set defaults for Prometheus metrics
	if config.PrometheusLocal.MetricsPort == 0 {
		config.PrometheusLocal.MetricsPort = 9090 // Default Prometheus metrics port
	}
	if config.PrometheusLocal.Labels == nil {
		config.PrometheusLocal.Labels = make(map[string]string)
	}

	// Set defaults for PrometheusRemoteWrite if enabled
	if config.PrometheusRemoteWrite.Enabled {
		if config.PrometheusRemoteWrite.Timeout == "" {
			config.PrometheusRemoteWrite.Timeout = "30s" // Default timeout for remote write
		}
		if config.PrometheusRemoteWrite.Headers == nil {
			config.PrometheusRemoteWrite.Headers = make(map[string]string)
		}

		if config.PrometheusRemoteWrite.TLSConfig.Enabled && !config.PrometheusRemoteWrite.TLSConfig.InsecureSkipVerify {
			config.PrometheusRemoteWrite.TLSConfig.InsecureSkipVerify = true // Default to true if not set
		}
	}

}

// validateConfig checks the configuration values for sanity
func validateConfig(config *Config) error {

	// Validate the iteration interval
	iterationInterval, err := time.ParseDuration(config.App.IterationInterval)
	if err != nil {
		return err
	}

	// Ensure the iteration interval is not lower than 15 seconds
	if iterationInterval < 15*time.Second {
		return errors.New("iteration_interval must be at least 15 seconds")
	}

	// Validate Redis configuration if Storage.Type is "redis"
	if config.Storage.Type == "redis" {
		if config.Storage.Redis.Address == "" {
			return errors.New("redis storage type selected, but address is missing")
		}
		if config.Storage.Redis.Port < 1 || config.Storage.Redis.Port > 65535 {
			return errors.New("redis port must be between 1 and 65535")
		}
		if config.Storage.Redis.ClientRequestTimeout == "" {
			return errors.New("redis client request timeout is missing")
		}
		if config.Storage.Redis.ClientIdleTimeout == "" {
			return errors.New("redis client idle timeout is missing")
		}
		if config.Storage.Redis.RetentionTTLSeconds < 3600 {
			return errors.New("redis retention_ttl_seconds must be greater than 3600 seconds")
		}
		// If authentication is enabled, ensure username and password are provided
		if config.Storage.Redis.Auth.Enabled {
			if config.Storage.Redis.Auth.Username == "" || config.Storage.Redis.Auth.Password == "" {
				return errors.New("redis auth enabled, but username or password is missing")
			}
		}
	} else {
		// For now, we only support Redis
		return fmt.Errorf("unsupported storage type: %s", config.Storage.Type)
	}

	// Validate each Kafka cluster configuration
	for _, cluster := range config.KafkaClusters {
		if len(cluster.Brokers) == 0 {
			return fmt.Errorf("kafka cluster '%s' must have at least one broker", cluster.Name)
		}

		// Validate SASL configuration if enabled
		if cluster.SASL.Enabled {
			if cluster.SASL.Mechanism != "SCRAM-SHA-256" && cluster.SASL.Mechanism != "SCRAM-SHA-512" {
				return fmt.Errorf("kafka cluster '%s' has invalid SASL mechanism; must be SCRAM-SHA-256 or SCRAM-SHA-512", cluster.Name)
			}
			if cluster.SASL.User == "" || cluster.SASL.Password == "" {
				return fmt.Errorf("kafka cluster '%s' has SASL enabled but missing user or password", cluster.Name)
			}
		}

	}

	// Set the default log level in logrus
	level, err := logrus.ParseLevel(strings.ToLower(config.App.LogLevel))
	if err != nil {
		logrus.Warnf("Invalid log level '%s', defaulting to 'info'", config.App.LogLevel)
		level = logrus.InfoLevel
	}
	logrus.SetLevel(level)

	// Validate Prometheus configuration
	if config.PrometheusLocal.MetricsPort == 0 {
		return errors.New("PrometheusLocal.MetricsPort is missing")
	}

	// PrometheusRemoteWrite
	if config.PrometheusRemoteWrite.Enabled {
		if config.PrometheusRemoteWrite.BasicAuth.Username == "" && config.PrometheusRemoteWrite.BearerToken == "" {
			return errors.New("either Basic Auth credentials or a Bearer Token must be provided for PrometheusRemoteWrite")
		}
		if config.PrometheusRemoteWrite.BasicAuth.Username != "" && config.PrometheusRemoteWrite.BearerToken != "" {
			return errors.New("only one authentication method should be used: either Basic Auth or Bearer Token, not both")
		}

		if config.PrometheusRemoteWrite.URL == "" {
			return errors.New("PrometheusRemoteWrite URL is required when enabled")
		}
		if _, err := time.ParseDuration(config.PrometheusRemoteWrite.Timeout); err != nil {
			return fmt.Errorf("invalid PrometheusRemoteWrite.Timeout: %v", err)
		}
	}
	return nil
}

// GetRedisAddress returns the Redis address in the format "host:port"
func (c *Config) GetRedisAddress() string {
	return fmt.Sprintf("%s:%d", c.Storage.Redis.Address, c.Storage.Redis.Port)
}

// GetIterationInterval returns the iteration interval as a time.Duration
func (c *Config) GetIterationInterval() (time.Duration, error) {
	return time.ParseDuration(c.App.IterationInterval)
}

// SetLogLevel sets the log level based on the configuration
func (c *Config) SetLogLevel() {
	level, err := logrus.ParseLevel(strings.ToLower(c.App.LogLevel))
	if err != nil {
		logrus.Warnf("Invalid log level '%s', defaulting to 'info'", c.App.LogLevel)
		level = logrus.InfoLevel
	}
	logrus.SetLevel(level)
}

// GetClusterConfig returns the KafkaCluster configuration for the given cluster name
func (c *Config) GetClusterConfig(clusterName string) (*KafkaCluster, error) {
	for _, cluster := range c.KafkaClusters {
		if cluster.Name == clusterName {
			return &cluster, nil
		}
	}
	return nil, fmt.Errorf("cluster %s not found in configuration", clusterName)
}
