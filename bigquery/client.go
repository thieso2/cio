package bigquery

import (
	"context"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/thieso2/cio/apilog"
)

var (
	// Singleton instance
	once      sync.Once
	bqClient  *bigquery.Client
	clientErr error
)

// GetClient returns a singleton BigQuery client instance
// The client is created once and reused for all operations
// Authentication uses Application Default Credentials (ADC)
func GetClient(ctx context.Context, projectID string) (*bigquery.Client, error) {
	once.Do(func() {
		apilog.Logf("[BQ] NewClient(project=%s)", projectID)
		bqClient, clientErr = bigquery.NewClient(ctx, projectID)
	})
	return bqClient, clientErr
}

// Close closes the BigQuery client if it was initialized
func Close() error {
	if bqClient != nil {
		return bqClient.Close()
	}
	return nil
}
