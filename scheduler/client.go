package scheduler

import (
	"context"

	"github.com/thieso2/cio/apilog"
	"github.com/thieso2/cio/gclient"
	cloudscheduler "google.golang.org/api/cloudscheduler/v1"
)

// provider holds the singleton Cloud Scheduler service.
var provider gclient.Provider[*cloudscheduler.Service]

// GetService returns the singleton Cloud Scheduler service.
func GetService(ctx context.Context) (*cloudscheduler.Service, error) {
	return provider.Get(ctx, func(ctx context.Context) (*cloudscheduler.Service, error) {
		apilog.Logf("[Scheduler] cloudscheduler.NewService()")
		return cloudscheduler.NewService(ctx)
	})
}

// Close is a no-op for the REST-based cloudscheduler service (kept for consistency).
func Close() {}
