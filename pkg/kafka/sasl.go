package kafka

import (
	"fmt"

	"github.com/sciclon2/kafka-lag-go/pkg/config"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

// XDGSCRAMClient is a struct that wraps the scram.Client and provides methods for SASL SCRAM authentication.
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

// Begin initializes the SCRAM client with the provided credentials and authorization ID.
func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

// Step processes the SCRAM challenge and returns the response.
func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

// Done checks if the SCRAM conversation is complete.
func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

// ConfigureSASL sets up SASL authentication for the Sarama client based on the provided configuration.
func ConfigureSASL(cfg *config.Config, saramaConfig *sarama.Config) error {
	kafkaConfig := cfg.Kafka

	saramaConfig.Net.SASL.Enable = true
	saramaConfig.Net.SASL.User = kafkaConfig.SASL.User
	saramaConfig.Net.SASL.Password = kafkaConfig.SASL.Password
	saramaConfig.Net.SASL.Handshake = true

	switch kafkaConfig.SASL.Mechanism {
	case "SCRAM-SHA-256":
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: scram.SHA256}
		}
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	case "SCRAM-SHA-512":
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: scram.SHA512}
		}
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	default:
		return fmt.Errorf("invalid SASL mechanism: %s", kafkaConfig.SASL.Mechanism)
	}

	return nil
}
