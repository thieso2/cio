package pubsub

import (
	"context"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/pubsub"
	"github.com/thieso2/cio/apilog"
	"github.com/thieso2/cio/gclient"
)

// Singleton providers for the Pub/Sub and Monitoring clients.
var (
	pubsubClient gclient.Provider[*pubsub.Client]
	monClient    gclient.Provider[*monitoring.MetricClient]
)

// GetClient returns the singleton Pub/Sub client for the given project.
func GetClient(ctx context.Context, projectID string) (*pubsub.Client, error) {
	return pubsubClient.Get(ctx, func(ctx context.Context) (*pubsub.Client, error) {
		apilog.Logf("[PubSub] NewClient(project=%s)", projectID)
		return pubsub.NewClient(ctx, projectID)
	})
}

// GetMonitoringClient returns the singleton Cloud Monitoring MetricClient.
func GetMonitoringClient(ctx context.Context) (*monitoring.MetricClient, error) {
	return monClient.Get(ctx, func(ctx context.Context) (*monitoring.MetricClient, error) {
		apilog.Logf("[PubSub] monitoring.NewMetricClient()")
		return monitoring.NewMetricClient(ctx)
	})
}

// Close closes all Pub/Sub clients.
func Close() {
	_ = pubsubClient.Close(func(c *pubsub.Client) error { return c.Close() })
	_ = monClient.Close(func(c *monitoring.MetricClient) error { return c.Close() })
}
