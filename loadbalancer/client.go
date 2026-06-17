package loadbalancer

import (
	"context"

	"github.com/thieso2/cio/apilog"
	"github.com/thieso2/cio/gclient"
	compute "google.golang.org/api/compute/v1"
)

// provider holds the singleton Compute REST API service (for LB resources).
var provider gclient.Provider[*compute.Service]

// GetService returns the singleton Compute REST API service (for LB resources).
func GetService(ctx context.Context) (*compute.Service, error) {
	return provider.Get(ctx, func(ctx context.Context) (*compute.Service, error) {
		apilog.Logf("[LoadBalancer] compute.NewService()")
		return compute.NewService(ctx)
	})
}

func Close() {}
