package cloudsql

import (
	"context"

	"github.com/thieso2/cio/apilog"
	"github.com/thieso2/cio/gclient"
	sqladmin "google.golang.org/api/sqladmin/v1beta4"
)

// provider holds the singleton Cloud SQL Admin service.
var provider gclient.Provider[*sqladmin.Service]

// GetService returns the singleton Cloud SQL Admin service.
func GetService(ctx context.Context) (*sqladmin.Service, error) {
	return provider.Get(ctx, func(ctx context.Context) (*sqladmin.Service, error) {
		apilog.Logf("[CloudSQL] sqladmin.NewService()")
		return sqladmin.NewService(ctx)
	})
}

// Close is a no-op for the REST-based sqladmin service (kept for consistency).
func Close() {}
