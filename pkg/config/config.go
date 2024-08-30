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
	Prometheus struct {
		MetricsPort int               `yaml:"metrics_port"`
		Labels      map[string]string `yaml:"labels,omitempty"`
	} `yaml:"prometheus"`
	Kafka struct {
		Brokers              []string `yaml:"brokers"`
		ClientRequestTimeout string   `yaml:"client_request_timeout"` // Timeout for Kafka client requests
		MetadataFetchTimeout string   `yaml:"metadata_fetch_timeout"` // Timeout for fetching metadata
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
	} `yaml:"kafka"`
	Storage struct {
		Type  string `yaml:"type"` // e.g., "redis", "mysql" (for future use)
		Redis struct {
			Address              string `yaml:"address"`
			Port                 int    `yaml:"port"`
			ClientRequestTimeout string `yaml:"client_request_timeout"`
			ClientIdleTimeout    string `yaml:"client_idle_timeout"`
			RetentionTTLSeconds  int    `yaml:"retention_ttl_seconds"`
		} `yaml:"redis"`
	} `yaml:"storage"`
	App struct {
		ClusterName       string `yaml:"cluster_name"`
		IterationInterval string `yaml:"iteration_interval"`
		NumWorkers        int    `yaml:"num_workers"`
		LogLevel          string `yaml:"log_level"`
		HealthCheckPort   int    `yaml:"health_check_port"`
		HealthCheckPath   string `yaml:"health_check_path"`
	} `yaml:"app"`
}

// GetConfigFilePath returns the path to the configuration file
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

// compileRegexPatterns compiles the regex patterns in the config
func compileRegexPatterns(config *Config) error {
	var err error

	// Compile whitelist pattern if it is not nil
	if config.Kafka.ConsumerGroups.Whitelist != nil {
		config.Kafka.ConsumerGroups.Whitelist, err = regexp.Compile(config.Kafka.ConsumerGroups.Whitelist.String())
		if err != nil {
			return fmt.Errorf("invalid consumer group whitelist regex pattern: %w", err)
		}
	}

	// Compile blacklist pattern if it is not nil
	if config.Kafka.ConsumerGroups.Blacklist != nil {
		config.Kafka.ConsumerGroups.Blacklist, err = regexp.Compile(config.Kafka.ConsumerGroups.Blacklist.String())
		if err != nil {
			return fmt.Errorf("invalid consumer group blacklist regex pattern: %w", err)
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
	if config.Kafka.ClientRequestTimeout == "" {
		config.Kafka.ClientRequestTimeout = "30s"
	}
	if config.Kafka.MetadataFetchTimeout == "" {
		config.Kafka.MetadataFetchTimeout = "5s"
	}
	// Handle consumer group regex patterns
	if config.Kafka.ConsumerGroups.Whitelist != nil && config.Kafka.ConsumerGroups.Whitelist.String() == "" {
		config.Kafka.ConsumerGroups.Whitelist = nil
	}
	if config.Kafka.ConsumerGroups.Blacklist != nil && config.Kafka.ConsumerGroups.Blacklist.String() == "" {
		config.Kafka.ConsumerGroups.Blacklist = nil
	}

	// Ensure SSL is enabled before considering InsecureSkipVerify
	if config.Kafka.SSL.Enabled {
		// If InsecureSkipVerify is not explicitly set, default it to true
		if !config.Kafka.SSL.InsecureSkipVerify {
			config.Kafka.SSL.InsecureSkipVerify = true
		}
	}
	if config.Prometheus.MetricsPort == 0 {
		config.Prometheus.MetricsPort = 9090 // Default Prometheus metrics port
	}
	if config.Prometheus.Labels == nil {
		config.Prometheus.Labels = make(map[string]string)
	}
	// Add the clusterName to the labels
	config.Prometheus.Labels["cluster_name"] = config.App.ClusterName

}

// validateConfig checks the configuration values for sanity
func validateConfig(config *Config) error {
	if config.App.ClusterName == "" {
		return errors.New("cluster_name is required and cannot be empty")
	}

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
	} else {
		// For now, we only support Redis
		return fmt.Errorf("unsupported storage type: %s", config.Storage.Type)
	}

	// Validate SASL configuration
	if config.Kafka.SASL.Enabled {
		if config.Kafka.SASL.Mechanism != "SCRAM-SHA-256" && config.Kafka.SASL.Mechanism != "SCRAM-SHA-512" {
			return errors.New("invalid SASL mechanism; must be SCRAM-SHA-256 or SCRAM-SHA-512")
		}
		if config.Kafka.SASL.User == "" || config.Kafka.SASL.Password == "" {
			return errors.New("sasl configuration is enabled but missing user or password")
		}
	}
	if config.Prometheus.Labels != nil {
		for key, value := range config.Prometheus.Labels {
			if key == "" || value == "" {
				return errors.New("prometheus labels keys and values must be non-empty strings")
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
