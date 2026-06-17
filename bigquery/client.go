package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/thieso2/cio/apilog"
	"github.com/thieso2/cio/gclient"
)

// provider holds the singleton BigQuery client. The projectID captured by the
// first GetClient call wins (once-semantics), matching the prior behaviour.
var provider gclient.Provider[*bigquery.Client]

// GetClient returns a singleton BigQuery client instance
// The client is created once and reused for all operations
// Authentication uses Application Default Credentials (ADC)
func GetClient(ctx context.Context, projectID string) (*bigquery.Client, error) {
	return provider.Get(ctx, func(ctx context.Context) (*bigquery.Client, error) {
		apilog.Logf("[BQ] NewClient(project=%s)", projectID)
		return bigquery.NewClient(ctx, projectID)
	})
}

// Close closes the BigQuery client if it was initialized
func Close() error {
	return provider.Close(func(c *bigquery.Client) error { return c.Close() })
}
