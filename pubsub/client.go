package pubsub

import (
	"context"
	"sync"

	"cloud.google.com/go/pubsub"
	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"github.com/thieso2/cio/apilog"
)

var (
	clientOnce sync.Once
	psClient   *pubsub.Client
	psErr      error

	monOnce   sync.Once
	monClient *monitoring.MetricClient
	monErr    error
)

// GetClient returns the singleton Pub/Sub client for the given project.
func GetClient(ctx context.Context, projectID string) (*pubsub.Client, error) {
	clientOnce.Do(func() {
		apilog.Logf("[PubSub] NewClient(project=%s)", projectID)
		psClient, psErr = pubsub.NewClient(ctx, projectID)
	})
	return psClient, psErr
}

// GetMonitoringClient returns the singleton Cloud Monitoring MetricClient.
func GetMonitoringClient(ctx context.Context) (*monitoring.MetricClient, error) {
	monOnce.Do(func() {
		apilog.Logf("[PubSub] monitoring.NewMetricClient()")
		monClient, monErr = monitoring.NewMetricClient(ctx)
	})
	return monClient, monErr
}

// Close closes all Pub/Sub clients.
func Close() {
	if psClient != nil {
		psClient.Close()
	}
	if monClient != nil {
		monClient.Close()
	}
}
