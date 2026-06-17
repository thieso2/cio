package compute

import (
	"context"

	computeapi "cloud.google.com/go/compute/apiv1"
	"github.com/thieso2/cio/apilog"
	"github.com/thieso2/cio/gclient"
)

// instances holds the singleton Compute Engine InstancesClient.
var instances gclient.Provider[*computeapi.InstancesClient]

// GetInstancesClient returns the singleton Compute Engine InstancesClient.
func GetInstancesClient(ctx context.Context) (*computeapi.InstancesClient, error) {
	return instances.Get(ctx, func(ctx context.Context) (*computeapi.InstancesClient, error) {
		apilog.Logf("[Compute] NewInstancesRESTClient()")
		return computeapi.NewInstancesRESTClient(ctx)
	})
}

// Close closes all Compute Engine clients.
func Close() {
	_ = instances.Close(func(c *computeapi.InstancesClient) error { return c.Close() })
}
