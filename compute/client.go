package compute

import (
	"context"
	"sync"

	computeapi "cloud.google.com/go/compute/apiv1"
	"github.com/thieso2/cio/apilog"
)

var (
	instancesOnce   sync.Once
	instancesClient *computeapi.InstancesClient
	instancesErr    error
)

// GetInstancesClient returns the singleton Compute Engine InstancesClient.
func GetInstancesClient(ctx context.Context) (*computeapi.InstancesClient, error) {
	instancesOnce.Do(func() {
		apilog.Logf("[Compute] NewInstancesRESTClient()")
		instancesClient, instancesErr = computeapi.NewInstancesRESTClient(ctx)
	})
	return instancesClient, instancesErr
}

// Close closes all Compute Engine clients.
func Close() {
	if instancesClient != nil {
		instancesClient.Close()
	}
}
