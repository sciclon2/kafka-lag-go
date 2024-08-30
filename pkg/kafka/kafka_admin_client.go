package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/sciclon2/kafka-lag-go/pkg/config"
	"github.com/sirupsen/logrus"

	"github.com/IBM/sarama"
)

const (
	// KafkaVersion defines the Kafka version used throughout the application.
	kafkaVersion = "2.1.0"
)

// KafkaClient is an interface that wraps sarama.Client methods.
type KafkaClient interface {
	Brokers() []*sarama.Broker
	Topics() ([]string, error)
	Partitions(topic string) ([]int32, error)
	GetOffset(topic string, partition int32, time int64) (int64, error)
	Leader(topic string, partition int32) (*sarama.Broker, error)
	Replicas(topic string, partition int32) ([]int32, error)
	RefreshMetadata(topics ...string) error
	Close() error
}

// KafkaAdmin is an interface that wraps sarama.ClusterAdmin methods.
type KafkaAdmin interface {
	ListConsumerGroups() (map[string]string, error)
	DescribeTopics(topics []string) ([]*sarama.TopicMetadata, error)
	ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error)
	ListTopics() (map[string]sarama.TopicDetail, error) // New method to list topics
	Close() error
}

func CreateAdminAndClient(cfg *config.Config, saramaConfig *sarama.Config) (KafkaClient, KafkaAdmin, error) {
	kafkaConfig := cfg.Kafka

	logrus.Debugf("Setting Kafka version to %s", kafkaVersion)
	if err := setKafkaVersion(kafkaVersion, saramaConfig); err != nil {
		return nil, nil, err
	}

	logrus.Debug("Parsing client request timeout")
	clientRequestTimeout, err := time.ParseDuration(cfg.Kafka.ClientRequestTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing ClientRequestTimeout: %w", err)
	}

	logrus.Debug("Parsing metadata fetch timeout")
	metadataTimeout, err := time.ParseDuration(cfg.Kafka.MetadataFetchTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing MetadataTimeout: %w", err)
	}

	saramaConfig.Net.DialTimeout = clientRequestTimeout
	saramaConfig.Metadata.Retry.Backoff = metadataTimeout

	if kafkaConfig.SSL.Enabled {
		logrus.Debug("Configuring TLS for Kafka connection")
		if err := configureTLS(cfg, saramaConfig); err != nil {
			return nil, nil, err
		}
	}

	if kafkaConfig.SASL.Enabled {
		logrus.Debug("Configuring SASL authentication for Kafka connection")
		if err := ConfigureSASL(cfg, saramaConfig); err != nil {
			return nil, nil, err
		}
	}

	logrus.Infof("Creating Kafka client with brokers: %v", cfg.Kafka.Brokers)
	client, err := sarama.NewClient(cfg.Kafka.Brokers, saramaConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating Kafka client with brokers %v: %w", cfg.Kafka.Brokers, err)
	}

	logrus.Debug("Creating Kafka admin client")
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		client.Close()
		return nil, nil, fmt.Errorf("error creating Kafka admin client: %w", err)
	}

	logrus.Info("Kafka client and admin successfully created")
	return client, admin, nil
}

func setKafkaVersion(versionStr string, saramaConfig *sarama.Config) error {
	logrus.Debugf("Setting Kafka version to %s", versionStr)
	version, err := sarama.ParseKafkaVersion(versionStr)
	if err != nil {
		return fmt.Errorf("error parsing Kafka version '%s': %w", versionStr, err)
	}
	saramaConfig.Version = version
	logrus.Debugf("Kafka version set to %s successfully", version)
	return nil
}

func configureTLS(cfg *config.Config, saramaConfig *sarama.Config) error {
	logrus.Debug("Configuring TLS settings")
	tlsConfig, err := createTLSConfiguration(cfg)
	if err != nil {
		return fmt.Errorf("error configuring TLS: %w", err)
	}
	saramaConfig.Net.TLS.Enable = true
	saramaConfig.Net.TLS.Config = tlsConfig
	logrus.Debug("TLS configuration applied successfully")
	return nil
}

func createTLSConfiguration(cfg *config.Config) (*tls.Config, error) {
	logrus.Debug("Creating TLS configuration")
	sslConfig := cfg.Kafka.SSL
	tlsConfig := &tls.Config{
		InsecureSkipVerify: sslConfig.InsecureSkipVerify,
	}

	caCertPool, err := loadCACertificate()
	if err != nil {
		return nil, err
	}
	tlsConfig.RootCAs = caCertPool

	if sslConfig.ClientCertificateFile != "" && sslConfig.ClientKeyFile != "" {
		logrus.Debug("Loading client certificate and key")
		if err := loadClientCertificate(sslConfig.ClientCertificateFile, sslConfig.ClientKeyFile, tlsConfig); err != nil {
			return nil, err
		}
	}

	logrus.Debug("TLS configuration created successfully")
	return tlsConfig, nil
}

// loadCACertificate loads the system's CA certificates.
func loadCACertificate() (*x509.CertPool, error) {
	// Load the system's CA certificates
	caCertPool, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("error loading system CA pool: %w", err)
	}

	return caCertPool, nil
}

// loadClientCertificate loads a client certificate and its corresponding private key from the specified files
// and applies them to the TLS configuration. This enables the Kafka client to authenticate itself to the Kafka brokers.
func loadClientCertificate(certFile, keyFile string, tlsConfig *tls.Config) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("error loading client certificate/key pair: %w", err)
	}
	tlsConfig.Certificates = []tls.Certificate{cert}
	return nil
}
