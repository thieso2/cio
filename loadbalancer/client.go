package loadbalancer

import (
	"context"
	"sync"

	"github.com/thieso2/cio/apilog"
	compute "google.golang.org/api/compute/v1"
)

var (
	serviceOnce sync.Once
	service     *compute.Service
	serviceErr  error
)

// GetService returns the singleton Compute REST API service (for LB resources).
func GetService(ctx context.Context) (*compute.Service, error) {
	serviceOnce.Do(func() {
		apilog.Logf("[LoadBalancer] compute.NewService()")
		service, serviceErr = compute.NewService(ctx)
	})
	return service, serviceErr
}

func Close() {}
