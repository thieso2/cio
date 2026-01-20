# Using cio as a Go Library

The `cio` project can be used as a library in your Go applications to interact with Google Cloud Storage and BigQuery.

## Installation

```bash
go get github.com/thieso2/cio
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/thieso2/cio/client"
)

func main() {
	// Create a new client
	c, err := client.New()
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()

	// List GCS objects
	objects, err := c.Storage().List(ctx, "gs://my-bucket/prefix/")
	if err != nil {
		log.Fatal(err)
	}

	for _, obj := range objects {
		fmt.Printf("%s (%d bytes)\n", obj.Name, obj.Size)
	}
}
```

## Available Packages

### High-Level API (`client`)

The `client` package provides a unified, high-level API:

```go
import "github.com/thieso2/cio/client"

// Create client
c, err := client.New()
defer c.Close()

// Access storage operations
c.Storage().List(ctx, "gs://bucket/")
c.Storage().UploadFile(ctx, localPath, gcsPath)
c.Storage().DownloadFile(ctx, gcsPath, localPath)

// Access BigQuery operations
c.BigQuery().ListDatasets(ctx, projectID)
c.BigQuery().ListTables(ctx, projectID, datasetID)
c.BigQuery().DescribeTable(ctx, projectID, datasetID, tableID)
```

### Low-Level Packages

For more control, you can use the individual packages directly:

#### Storage Package

```go
import "github.com/thieso2/cio/storage"

// List objects
objects, err := storage.List(ctx, "bucket-name", "prefix/")

// Upload file
err = storage.UploadFile(ctx, "/local/file.txt", "bucket-name", "remote/file.txt")

// Download file
err = storage.DownloadFile(ctx, "bucket-name", "remote/file.txt", "/local/file.txt")

// Remove object
err = storage.RemoveObject(ctx, "bucket-name", "remote/file.txt")

// Clean up
storage.Close()
```

#### BigQuery Package

```go
import "github.com/thieso2/cio/bigquery"

// List datasets
datasets, err := bigquery.ListDatasets(ctx, "project-id")

// List tables
tables, err := bigquery.ListTables(ctx, "project-id", "dataset-id")

// Get table schema
table, err := bigquery.DescribeTable(ctx, "project-id", "dataset-id", "table-id")

// Remove table
err = bigquery.RemoveTable(ctx, "project-id", "dataset-id", "table-id")

// Clean up
bigquery.Close()
```

#### Config Package

```go
import "github.com/thieso2/cio/config"

// Load configuration
cfg, err := config.Load("/path/to/config.yaml")

// Access mappings
fmt.Println(cfg.Mappings) // map[string]string

// Access defaults
fmt.Println(cfg.Defaults.ProjectID)
fmt.Println(cfg.Defaults.Region)
```

#### Resolver Package

```go
import (
	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/config"
)

// Create resolver with config
cfg, _ := config.Load("")
r := resolver.New(cfg)

// Resolve alias to full path
fullPath, err := r.Resolve(":am/2024/")
// Returns: "gs://bucket-name/2024/"

// Reverse resolve (full path to alias)
aliasPath := r.ReverseResolve("gs://bucket-name/2024/file.txt")
// Returns: ":am/2024/file.txt"

// Parse paths
bucket, prefix, err := resolver.ParseGCSPath("gs://bucket/prefix/")
projectID, datasetID, tableID, err := resolver.ParseBQPath("bq://project.dataset.table")
```

#### Resource Package

```go
import (
	"github.com/thieso2/cio/resource"
	"github.com/thieso2/cio/resolver"
)

// Create resource factory
r := resolver.New(cfg)
factory := resource.NewFactory(r.ReverseResolve)

// Create resource handler
res, err := factory.Create("gs://bucket/prefix/")

// List resources
resources, err := res.List(ctx, "gs://bucket/", &resource.ListOptions{
	Recursive: true,
	MaxResults: 100,
})

// Format output
for _, info := range resources {
	fmt.Println(res.FormatShort(info, ":am/file.txt"))
	fmt.Println(res.FormatLong(info, ":am/file.txt"))
}
```

## Configuration

### Using Default Configuration

The client automatically loads configuration from:
1. `~/.config/cio/config.yaml`
2. `~/.cio/config.yaml`
3. Environment variable `CIO_CONFIG`

```go
// Uses default config locations
c, err := client.New()
```

### Using Custom Configuration

```go
c, err := client.New(client.Options{
	ConfigPath: "/custom/path/config.yaml",
	ProjectID:  "override-project",
	Region:     "override-region",
})
```

### Configuration File Format

```yaml
mappings:
  am: gs://io-spooler-onprem-archived-metrics/
  logs: gs://my-project-logs/
  mydata: bq://my-project-id.my-dataset

defaults:
  project_id: ${GCP_PROJECT}
  region: europe-west3
```

## Alias Resolution

Aliases provide a convenient way to reference resources:

```go
// List using alias (if :am is mapped in config)
objects, err := c.Storage().List(ctx, ":am/2024/")

// Upload using alias
err = c.Storage().UploadFile(ctx, "file.txt", ":am/uploads/file.txt")
```

## Authentication

The library uses Google Application Default Credentials (ADC):

1. `GOOGLE_APPLICATION_CREDENTIALS` environment variable
2. User credentials from `gcloud auth application-default login`
3. Service account credentials (on GCE, Cloud Run, etc.)

```bash
# Setup authentication
gcloud auth application-default login

# Or use service account
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

## Error Handling

All methods return errors that should be checked:

```go
objects, err := c.Storage().List(ctx, "gs://bucket/")
if err != nil {
	// Handle error
	log.Printf("Failed to list objects: %v", err)
	return err
}
```

## Resource Cleanup

Always close the client when done:

```go
c, err := client.New()
if err != nil {
	return err
}
defer c.Close() // Important: releases GCS and BigQuery clients
```

## Examples

See [client/example_test.go](client/example_test.go) for comprehensive examples including:
- Basic usage
- Custom configuration
- Alias resolution
- Upload/download operations
- BigQuery operations
- Wildcard patterns

## Thread Safety

The client and underlying packages are safe for concurrent use. The singleton pattern used for GCS and BigQuery clients ensures efficient resource usage across multiple goroutines.

## Performance Considerations

- **Connection Pooling**: The storage and BigQuery clients use connection pooling automatically
- **Context Usage**: Always pass contexts for cancellation support
- **Batch Operations**: Use wildcard patterns for bulk operations
- **Resource Cleanup**: Call `Close()` to release resources properly

## Go Module

Add to your `go.mod`:

```go
require github.com/thieso2/cio v0.1.0
```

## Documentation

Full API documentation is available at:
- Client API: [client/client.go](client/client.go)
- Storage: [storage/](storage/)
- BigQuery: [bigquery/](bigquery/)
- Config: [config/](config/)
- Resolver: [resolver/](resolver/)
- Resource: [resource/](resource/)

## Support

For issues and questions:
- GitHub Issues: https://github.com/thieso2/cio/issues
- CLI Documentation: [README.md](README.md)
