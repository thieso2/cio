package certmanager

import (
	"context"

	"github.com/thieso2/cio/apilog"
	"github.com/thieso2/cio/gclient"
	cm "google.golang.org/api/certificatemanager/v1"
)

// provider holds the singleton Certificate Manager service.
var provider gclient.Provider[*cm.Service]

// GetService returns the singleton Certificate Manager service.
func GetService(ctx context.Context) (*cm.Service, error) {
	return provider.Get(ctx, func(ctx context.Context) (*cm.Service, error) {
		apilog.Logf("[CertManager] certificatemanager.NewService()")
		return cm.NewService(ctx)
	})
}

func Close() {}
