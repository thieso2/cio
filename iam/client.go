package iam

import (
	"context"
	"sync"

	"github.com/thieso2/cio/apilog"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
)

var (
	client     *iam.Service
	clientOnce sync.Once
	clientErr  error
)

// GetClient returns a singleton IAM client.
// The client is initialized on first call and reused for subsequent calls.
func GetClient(ctx context.Context, opts ...option.ClientOption) (*iam.Service, error) {
	clientOnce.Do(func() {
		apilog.Logf("[IAM] NewService()")
		client, clientErr = iam.NewService(ctx, opts...)
	})
	return client, clientErr
}

// Close closes the IAM client.
// Note: The IAM client doesn't require explicit cleanup, but this is provided
// for consistency with other clients.
func Close() error {
	// IAM client doesn't need explicit cleanup
	return nil
}
