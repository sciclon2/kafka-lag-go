package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"regexp"
	"time"

	"gopkg.in/yaml.v2"
)

// Config represents the application configuration
type Config struct {
	Prometheus struct {
		MetricsPort int `yaml:"metrics_port"`
	} `yaml:"prometheus"`
	Kafka struct {
		Version        string   `yaml:"version"`
		Brokers        []string `yaml:"brokers"`
		ConsumerGroups struct {
			Whitelist *regexp.Regexp `yaml:"whitelist"`
			Blacklist *regexp.Regexp `yaml:"blacklist"`
		} `yaml:"consumer_groups"`
	} `yaml:"kafka"`
	Redis struct {
		Address string `yaml:"address"`
		Port    int    `yaml:"port"`
	} `yaml:"redis"`
	App struct {
		IterationInterval string `yaml:"iteration_interval"`
		NumWorkers        int    `yaml:"num_workers"`
	} `yaml:"app"`
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
	if config.Redis.Address == "" {
		config.Redis.Address = "localhost"
	}
	if config.Redis.Port == 0 {
		config.Redis.Port = 6379
	}
	if len(config.Kafka.Brokers) == 0 {
		config.Kafka.Brokers = []string{"localhost:9092"}
	}
	if config.Kafka.Version == "" {
		config.Kafka.Version = "2.1.0"
	}
	if config.App.IterationInterval == "" {
		config.App.IterationInterval = "30s"
	}
	if config.App.NumWorkers == 0 {
		config.App.NumWorkers = 5
	}
	// Handle consumer group regex patterns
	if config.Kafka.ConsumerGroups.Whitelist != nil && config.Kafka.ConsumerGroups.Whitelist.String() == "" {
		config.Kafka.ConsumerGroups.Whitelist = nil
	}
	if config.Kafka.ConsumerGroups.Blacklist != nil && config.Kafka.ConsumerGroups.Blacklist.String() == "" {
		config.Kafka.ConsumerGroups.Blacklist = nil
	}
}

// GetIterationInterval returns the iteration interval as a time.Duration
func (c *Config) GetIterationInterval() (time.Duration, error) {
	return time.ParseDuration(c.App.IterationInterval)
}

// validateConfig checks the configuration values for sanity
func validateConfig(config *Config) error {
	iterationInterval, err := time.ParseDuration(config.App.IterationInterval)
	if err != nil {
		return err
	}

	// Ensure the iteration interval is not lower than 15 seconds
	if iterationInterval < 15*time.Second {
		return errors.New("iteration_interval must be at least 15 seconds")
	}

	// Validate the Redis port range
	if config.Redis.Port < 1 || config.Redis.Port > 65535 {
		return errors.New("redis port must be between 1 and 65535")
	}

	return nil
}

// GetRedisAddress returns the Redis address in the format "host:port"
func (c *Config) GetRedisAddress() string {
	return fmt.Sprintf("%s:%d", c.Redis.Address, c.Redis.Port)
}
