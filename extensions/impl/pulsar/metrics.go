package pulsar

import (
	"github.com/lf-edge/ekuiper/v2/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	LblTarget = "target"
)

var (
	PulsarSinkCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pulsar",
			Subsystem: "sink",
			Name:      "messages_total",
			Help:      "Total number of messages sent to Pulsar",
		},
		[]string{metrics.LblType, LblTarget, metrics.LblRuleIDType, metrics.LblOpIDType},
	)

	PulsarSinkCollectorDurationHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pulsar",
			Subsystem: "pulsar_sink",
			Name:      "collector_duration_hist",
			Help:      "Sink Histogram Duration of IO",
			Buckets:   prometheus.ExponentialBuckets(10, 2, 10),
		}, []string{metrics.LblType, LblTarget, metrics.LblRuleIDType, metrics.LblOpIDType},
	)

	PulsarSourceCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pulsar",
			Subsystem: "pulsar_source",
			Name:      "messages_total",
			Help:      "Total number of messages received from Pulsar",
		}, []string{metrics.LblType, LblTarget, metrics.LblRuleIDType, metrics.LblOpIDType},
	)
)
