package cloudsql

import (
	"context"
	"sync"

	"github.com/thieso2/cio/apilog"
	sqladmin "google.golang.org/api/sqladmin/v1beta4"
)

var (
	serviceOnce sync.Once
	service     *sqladmin.Service
	serviceErr  error
)

// GetService returns the singleton Cloud SQL Admin service.
func GetService(ctx context.Context) (*sqladmin.Service, error) {
	serviceOnce.Do(func() {
		apilog.Logf("[CloudSQL] sqladmin.NewService()")
		service, serviceErr = sqladmin.NewService(ctx)
	})
	return service, serviceErr
}

// Close is a no-op for the REST-based sqladmin service (kept for consistency).
func Close() {}
