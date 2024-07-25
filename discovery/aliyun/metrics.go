package aliyun

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/discovery"
)

var _ discovery.DiscovererMetrics = (*ecsMetrics)(nil)

type ecsMetrics struct {
	refreshMetrics discovery.RefreshMetricsInstantiator

	queryCount         prometheus.Counter
	queryFailuresCount prometheus.Counter

	metricRegisterer discovery.MetricRegisterer
}

func newDiscovererMetrics(reg prometheus.Registerer, rmi discovery.RefreshMetricsInstantiator) discovery.DiscovererMetrics {
	m := &ecsMetrics{
		refreshMetrics: rmi,
		queryCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "prometheus_sd_ecs_query_total",
				Help: "Number of aliyun ecs service discovery refresh.",
			}),
		queryFailuresCount: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "prometheus_sd_ecs_query_failures_total",
				Help: "Number of aliyun ecs service discovery refresh failures.",
			}),
	}

	m.metricRegisterer = discovery.NewMetricRegisterer(reg, []prometheus.Collector{
		m.queryCount,
		m.queryFailuresCount,
	})

	return m
}

// Register implements discovery.DiscovererMetrics.
func (m *ecsMetrics) Register() error {
	return m.metricRegisterer.RegisterMetrics()
}

// Unregister implements discovery.DiscovererMetrics.
func (m *ecsMetrics) Unregister() {
	m.metricRegisterer.UnregisterMetrics()
}
