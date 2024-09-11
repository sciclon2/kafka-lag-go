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
	"github.com/sciclon2/kafka-lag-go/pkg/healthcheck"
	"github.com/sciclon2/kafka-lag-go/pkg/kafka"
	"github.com/sciclon2/kafka-lag-go/pkg/structs"

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

func InitializeKafkaClient(cfg *config.Config) (map[string]structs.KafkaClient, map[string]structs.KafkaAdmin, map[string]*sarama.Config) {
	clientMap := make(map[string]structs.KafkaClient)
	adminMap := make(map[string]structs.KafkaAdmin)
	saramaConfigMap := make(map[string]*sarama.Config)

	for _, cluster := range cfg.KafkaClusters {
		saramaConfig := sarama.NewConfig()

		client, admin, err := kafka.CreateAdminAndClient(cluster, saramaConfig)
		if err != nil {
			logrus.Fatalf("Failed to create Kafka client and admin for cluster '%s': %v", cluster.Name, err)
		}

		clientMap[cluster.Name] = client
		adminMap[cluster.Name] = admin
		saramaConfigMap[cluster.Name] = saramaConfig

	}

	return clientMap, adminMap, saramaConfigMap
}

func DeferCloseClientsAndAdmins(clientMap map[string]structs.KafkaClient, adminMap map[string]structs.KafkaAdmin) {
	for clusterName, client := range clientMap {
		defer func(c structs.KafkaClient, name string) {
			if err := c.Close(); err != nil {
				logrus.Errorf("Error closing Kafka client for cluster %s: %v", name, err)
			}
		}(client, clusterName)
	}

	for clusterName, admin := range adminMap {
		defer func(a structs.KafkaAdmin, name string) {
			if err := a.Close(); err != nil {
				logrus.Errorf("Error closing Kafka admin for cluster %s: %v", name, err)
			}
		}(admin, clusterName)
	}
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
		address := fmt.Sprintf(":%d", cfg.PrometheusLocal.MetricsPort)
		logrus.Infof("Starting Prometheus metrics server on port %d", cfg.PrometheusLocal.MetricsPort)
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
func InitializeAndStartHealthcheck(kafkaAdmins map[string]structs.KafkaAdmin, store storage.Storage, interval time.Duration, cfg *config.Config) *healthcheck.ApplicationHealthchech {
	applicationHeartbeat := healthcheck.NewApplicationHealthcheck(kafkaAdmins, store, interval, cfg.App.HealthCheckPort, cfg.App.HealthCheckPath)
	applicationHeartbeat.Start(3 * time.Second)
	return applicationHeartbeat
}
