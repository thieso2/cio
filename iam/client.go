package iam

import (
	"context"

	"github.com/thieso2/cio/apilog"
	"github.com/thieso2/cio/gclient"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
)

// provider holds the singleton IAM service.
var provider gclient.Provider[*iam.Service]

// GetClient returns a singleton IAM client.
// The client is initialized on first call and reused for subsequent calls.
func GetClient(ctx context.Context, opts ...option.ClientOption) (*iam.Service, error) {
	return provider.Get(ctx, func(ctx context.Context) (*iam.Service, error) {
		apilog.Logf("[IAM] NewService()")
		return iam.NewService(ctx, opts...)
	})
}

// Close closes the IAM client.
// Note: The IAM client doesn't require explicit cleanup, but this is provided
// for consistency with other clients.
func Close() error {
	// IAM client doesn't need explicit cleanup
	return nil
}
