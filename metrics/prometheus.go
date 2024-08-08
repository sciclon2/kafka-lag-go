package metrics

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
var (
	KafkaTimeLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_time_lag_seconds",
			Help: "Kafka time lag in seconds by group, topic, and partition.",
		},
		[]string{"group", "topic", "partition"},
	)
)

func init() {
	// Register Prometheus metrics
	prometheus.MustRegister(KafkaLagOffset)
	prometheus.MustRegister(KafkaTimeLag)
}
