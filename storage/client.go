package storage

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/thieso2/cio/apilog"
	"github.com/thieso2/cio/gclient"
)

// provider holds the singleton GCS client.
var provider gclient.Provider[*storage.Client]

// GetClient returns a singleton GCS client instance
// The client is created once and reused for all operations
// Authentication uses Application Default Credentials (ADC)
func GetClient(ctx context.Context) (*storage.Client, error) {
	return provider.Get(ctx, func(ctx context.Context) (*storage.Client, error) {
		apilog.Logf("[GCS] NewClient()")
		return storage.NewClient(ctx)
	})
}

// Close closes the GCS client if it was initialized
func Close() error {
	return provider.Close(func(c *storage.Client) error { return c.Close() })
}
