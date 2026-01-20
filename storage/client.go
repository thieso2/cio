package storage

import (
	"context"
	"sync"

	"cloud.google.com/go/storage"
)

var (
	// Singleton instance
	once      sync.Once
	gcsClient *storage.Client
	clientErr error
)

// GetClient returns a singleton GCS client instance
// The client is created once and reused for all operations
// Authentication uses Application Default Credentials (ADC)
func GetClient(ctx context.Context) (*storage.Client, error) {
	once.Do(func() {
		gcsClient, clientErr = storage.NewClient(ctx)
	})
	return gcsClient, clientErr
}

// Close closes the GCS client if it was initialized
func Close() error {
	if gcsClient != nil {
		return gcsClient.Close()
	}
	return nil
}
