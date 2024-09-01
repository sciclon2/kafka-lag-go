package maininit

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sciclon2/kafka-lag-go/pkg/config"
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
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		logrus.Fatal(http.ListenAndServe(":"+strconv.Itoa(cfg.Prometheus.MetricsPort), nil))
	}()
}

// GetConfigFilePath retrieves the configuration file path.
func GetConfigFilePath() string {
	// Logic to get the configuration file path.
	return config.GetConfigFilePath()
}

func InitializeSignalHandling() chan os.Signal {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGUSR1)
	return sigChan
}
