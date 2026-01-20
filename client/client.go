// Package client provides a high-level API for interacting with Google Cloud Platform resources.
//
// This package offers a unified interface for working with Google Cloud Storage (GCS) and
// BigQuery, with support for alias-based path resolution and convenient helper methods.
//
// Example usage:
//
//	// Create a new client
//	client, err := client.New()
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	// List GCS objects
//	objects, err := client.Storage().List(ctx, "gs://bucket/prefix/")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// List BigQuery tables
//	tables, err := client.BigQuery().ListTables(ctx, "project-id", "dataset")
//	if err != nil {
//		log.Fatal(err)
//	}
package client

import (
	"context"
	"fmt"

	"github.com/thieso2/cio/bigquery"
	"github.com/thieso2/cio/config"
	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/storage"
)

// Client provides a high-level API for interacting with GCP resources.
type Client struct {
	config   *config.Config
	resolver *resolver.Resolver
}

// Options configures the client.
type Options struct {
	// ConfigPath specifies the path to the configuration file.
	// If empty, the default configuration locations will be used.
	ConfigPath string

	// ProjectID overrides the default project ID from the configuration.
	ProjectID string

	// Region overrides the default region from the configuration.
	Region string
}

// New creates a new Client with the given options.
func New(opts ...Options) (*Client, error) {
	var opt Options
	if len(opts) > 0 {
		opt = opts[0]
	}

	// Load configuration
	cfg, err := config.Load(opt.ConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// Apply overrides
	if opt.ProjectID != "" {
		cfg.Defaults.ProjectID = opt.ProjectID
	}
	if opt.Region != "" {
		cfg.Defaults.Region = opt.Region
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Create resolver
	r := resolver.New(cfg)

	return &Client{
		config:   cfg,
		resolver: r,
	}, nil
}

// Config returns the client's configuration.
func (c *Client) Config() *config.Config {
	return c.config
}

// Resolver returns the client's path resolver for alias resolution.
func (c *Client) Resolver() *resolver.Resolver {
	return c.resolver
}

// Storage returns a storage client for GCS operations.
func (c *Client) Storage() *StorageClient {
	return &StorageClient{
		config:   c.config,
		resolver: c.resolver,
	}
}

// BigQuery returns a BigQuery client for BigQuery operations.
func (c *Client) BigQuery() *BigQueryClient {
	return &BigQueryClient{
		config:   c.config,
		resolver: c.resolver,
	}
}

// Close releases any resources held by the client.
func (c *Client) Close() error {
	// Close storage client if initialized
	if err := storage.Close(); err != nil {
		return fmt.Errorf("failed to close storage client: %w", err)
	}

	// Close BigQuery client if initialized
	if err := bigquery.Close(); err != nil {
		return fmt.Errorf("failed to close BigQuery client: %w", err)
	}

	return nil
}

// StorageClient provides methods for interacting with Google Cloud Storage.
type StorageClient struct {
	config   *config.Config
	resolver *resolver.Resolver
}

// List lists objects in a GCS bucket with optional prefix filtering.
// The path can be a full GCS path (gs://bucket/prefix/) or an alias (:alias/prefix/).
func (s *StorageClient) List(ctx context.Context, path string) ([]*storage.ObjectInfo, error) {
	// Resolve alias if present
	fullPath, err := s.resolver.Resolve(path)
	if err != nil {
		return nil, err
	}

	// Parse GCS path
	bucket, prefix, err := resolver.ParseGCSPath(fullPath)
	if err != nil {
		return nil, err
	}

	// List objects
	return storage.List(ctx, bucket, prefix)
}

// ListWithPattern lists objects matching a wildcard pattern.
func (s *StorageClient) ListWithPattern(ctx context.Context, pattern string) ([]*storage.ObjectInfo, error) {
	// Resolve alias if present
	fullPattern, err := s.resolver.Resolve(pattern)
	if err != nil {
		return nil, err
	}

	// Parse GCS path
	bucket, prefix, err := resolver.ParseGCSPath(fullPattern)
	if err != nil {
		return nil, err
	}

	return storage.ListWithPattern(ctx, bucket, prefix)
}

// DownloadFile downloads a single file from GCS to a local path.
func (s *StorageClient) DownloadFile(ctx context.Context, gcsPath, localPath string) error {
	// Resolve alias if present
	fullPath, err := s.resolver.Resolve(gcsPath)
	if err != nil {
		return err
	}

	// Parse GCS path
	bucket, object, err := resolver.ParseGCSPath(fullPath)
	if err != nil {
		return err
	}

	return storage.DownloadFile(ctx, bucket, object, localPath)
}

// UploadFile uploads a local file to GCS.
func (s *StorageClient) UploadFile(ctx context.Context, localPath, gcsPath string) error {
	// Resolve alias if present
	fullPath, err := s.resolver.Resolve(gcsPath)
	if err != nil {
		return err
	}

	// Parse GCS path
	bucket, object, err := resolver.ParseGCSPath(fullPath)
	if err != nil {
		return err
	}

	return storage.UploadFile(ctx, localPath, bucket, object)
}

// RemoveObject removes a single object from GCS.
func (s *StorageClient) RemoveObject(ctx context.Context, gcsPath string) error {
	// Resolve alias if present
	fullPath, err := s.resolver.Resolve(gcsPath)
	if err != nil {
		return err
	}

	// Parse GCS path
	bucket, object, err := resolver.ParseGCSPath(fullPath)
	if err != nil {
		return err
	}

	return storage.RemoveObject(ctx, bucket, object)
}

// BigQueryClient provides methods for interacting with Google BigQuery.
type BigQueryClient struct {
	config   *config.Config
	resolver *resolver.Resolver
}

// ListDatasets lists all datasets in a project.
func (b *BigQueryClient) ListDatasets(ctx context.Context, projectID string) ([]*bigquery.BQObjectInfo, error) {
	if projectID == "" {
		projectID = b.config.Defaults.ProjectID
	}
	return bigquery.ListDatasets(ctx, projectID)
}

// ListTables lists all tables in a dataset.
func (b *BigQueryClient) ListTables(ctx context.Context, projectID, datasetID string) ([]*bigquery.BQObjectInfo, error) {
	if projectID == "" {
		projectID = b.config.Defaults.ProjectID
	}
	return bigquery.ListTables(ctx, projectID, datasetID)
}

// DescribeTable returns detailed information about a table.
func (b *BigQueryClient) DescribeTable(ctx context.Context, projectID, datasetID, tableID string) (*bigquery.BQObjectInfo, error) {
	if projectID == "" {
		projectID = b.config.Defaults.ProjectID
	}
	return bigquery.DescribeTable(ctx, projectID, datasetID, tableID)
}

// RemoveTable removes a table from BigQuery.
func (b *BigQueryClient) RemoveTable(ctx context.Context, projectID, datasetID, tableID string) error {
	if projectID == "" {
		projectID = b.config.Defaults.ProjectID
	}
	return bigquery.RemoveTable(ctx, projectID, datasetID, tableID)
}
