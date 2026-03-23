package certmanager

import (
	"context"
	"sync"

	"github.com/thieso2/cio/apilog"
	cm "google.golang.org/api/certificatemanager/v1"
)

var (
	serviceOnce sync.Once
	service     *cm.Service
	serviceErr  error
)

// GetService returns the singleton Certificate Manager service.
func GetService(ctx context.Context) (*cm.Service, error) {
	serviceOnce.Do(func() {
		apilog.Logf("[CertManager] certificatemanager.NewService()")
		service, serviceErr = cm.NewService(ctx)
	})
	return service, serviceErr
}

func Close() {}
