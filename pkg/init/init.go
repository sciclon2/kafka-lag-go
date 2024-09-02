package maininit

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sciclon2/kafka-lag-go/pkg/config"
	"github.com/sciclon2/kafka-lag-go/pkg/heartbeat"
	"github.com/sciclon2/kafka-lag-go/pkg/kafka"
	"github.com/sciclon2/kafka-lag-go/pkg/storage"
	"github.com/sirupsen/logrus"
)

func InitializeConfigAndLogging() *config.Config {
	// Internally resolve the configuration file path
	configPath := config.GetConfigFilePath()

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	cfg.SetLogLevel()
	return cfg
}

// Return the kafka.KafkaClient type
func InitializeKafkaClient(cfg *config.Config) (kafka.KafkaClient, kafka.KafkaAdmin, *sarama.Config) {
	saramaConfig := sarama.NewConfig()
	client, admin, err := kafka.CreateAdminAndClient(cfg, saramaConfig)
	if err != nil {
		logrus.Fatalf("%v", err)
	}
	return client, admin, saramaConfig
}

func InitializeStorage(cfg *config.Config) storage.Storage {
	store, err := storage.InitializeStorage(cfg)
	if err != nil {
		logrus.Fatalf("Failed to initialize storage: %v\n", err)
	}
	return store
}

func InitializeMetricsServer(cfg *config.Config) {
	// Create a new ServeMux for metrics
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())

	// Start the Prometheus metrics server
	go func() {
		address := fmt.Sprintf(":%d", cfg.Prometheus.MetricsPort)
		logrus.Infof("Starting Prometheus metrics server on port %d", cfg.Prometheus.MetricsPort)
		if err := http.ListenAndServe(address, metricsMux); err != nil {
			logrus.Fatalf("Failed to start Prometheus metrics server: %v", err)
		}
	}()
}

func InitializeSignalHandling() chan os.Signal {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGUSR1)
	return sigChan
}

// initializeAndStartHeartbeat initializes the ApplicationHeartbeat and starts the health check routine.
func InitializeAndStartHeartbeat(kafkaAdmin kafka.KafkaAdmin, store storage.Storage, interval time.Duration, cfg *config.Config) *heartbeat.ApplicationHeartbeat {
	applicationHeartbeat := heartbeat.NewApplicationHeartbeat(kafkaAdmin, store, interval, cfg.App.HealthCheckPort, cfg.App.HealthCheckPath)
	applicationHeartbeat.Start()
	return applicationHeartbeat
}
