package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/sciclon2/kafka-lag-go/pkg/config"
	"github.com/sciclon2/kafka-lag-go/pkg/structs"

	"github.com/sirupsen/logrus"

	"github.com/IBM/sarama"
)

const (
	// KafkaVersion defines the Kafka version used throughout the application.
	kafkaVersion = "2.1.0"
)

func CreateAdminAndClient(cluster config.KafkaCluster, saramaConfig *sarama.Config) (structs.KafkaClient, structs.KafkaAdmin, error) {
	logrus.Debugf("Setting Kafka version to %s", kafkaVersion)
	if err := setKafkaVersion(kafkaVersion, saramaConfig); err != nil {
		return nil, nil, err
	}

	logrus.Debug("Parsing client request timeout")
	clientRequestTimeout, err := time.ParseDuration(cluster.ClientRequestTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing ClientRequestTimeout: %w", err)
	}

	logrus.Debug("Parsing metadata fetch timeout")
	metadataTimeout, err := time.ParseDuration(cluster.MetadataFetchTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing MetadataTimeout: %w", err)
	}

	saramaConfig.Net.DialTimeout = clientRequestTimeout
	saramaConfig.Metadata.Retry.Backoff = metadataTimeout

	if cluster.SSL.Enabled {
		logrus.Debug("Configuring TLS for Kafka connection")
		if err := configureTLS(cluster, saramaConfig); err != nil {
			return nil, nil, err
		}
	}

	if cluster.SASL.Enabled {
		logrus.Debug("Configuring SASL authentication for Kafka connection")
		if err := ConfigureSASL(cluster, saramaConfig); err != nil {
			return nil, nil, err
		}
	}

	logrus.Infof("Creating Kafka client with brokers: %v", cluster.Brokers)
	client, err := sarama.NewClient(cluster.Brokers, saramaConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating Kafka client with brokers %v: %w", cluster.Brokers, err)
	}

	logrus.Debug("Creating Kafka admin client")
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		client.Close()
		return nil, nil, fmt.Errorf("error creating Kafka admin client: %w", err)
	}

	// Wrap the real sarama client and admin in the custom structs that implement the interfaces.
	return &structs.SaramaKafkaClient{Client: client}, &structs.SaramaKafkaAdmin{Admin: admin}, nil
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

func configureTLS(cluster config.KafkaCluster, saramaConfig *sarama.Config) error {
	logrus.Debug("Configuring TLS settings")
	tlsConfig, err := createTLSConfiguration(cluster)
	if err != nil {
		return fmt.Errorf("error configuring TLS for cluster '%s': %w", cluster.Name, err)
	}
	saramaConfig.Net.TLS.Enable = true
	saramaConfig.Net.TLS.Config = tlsConfig
	logrus.Debugf("TLS configuration applied successfully for cluster '%s'", cluster.Name)
	return nil
}

func createTLSConfiguration(cluster config.KafkaCluster) (*tls.Config, error) {
	logrus.Debugf("Creating TLS configuration for cluster '%s'", cluster.Name)
	sslConfig := cluster.SSL
	tlsConfig := &tls.Config{
		InsecureSkipVerify: sslConfig.InsecureSkipVerify,
	}

	// Load the CA certificate if required
	caCertPool, err := loadCACertificate()
	if err != nil {
		return nil, err
	}
	tlsConfig.RootCAs = caCertPool

	// Load the client certificate and key if specified
	if sslConfig.ClientCertificateFile != "" && sslConfig.ClientKeyFile != "" {
		logrus.Debugf("Loading client certificate and key for cluster '%s'", cluster.Name)
		if err := loadClientCertificate(sslConfig.ClientCertificateFile, sslConfig.ClientKeyFile, tlsConfig); err != nil {
			return nil, err
		}
	}

	logrus.Debugf("TLS configuration created successfully for cluster '%s'", cluster.Name)
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
