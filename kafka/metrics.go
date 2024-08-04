package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Define Prometheus metrics
var (
	KafkaLagOffset = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_lag_offset",
			Help: "Kafka lag offset by group, topic, and partition.",
		},
		[]string{"group", "topic", "partition"},
	)
)

func init() {
	// Register Prometheus metrics
	prometheus.MustRegister(KafkaLagOffset)
}
