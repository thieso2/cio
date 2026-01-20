package client_test

import (
	"context"
	"fmt"
	"log"

	"github.com/thieso2/cio/client"
)

// Example_basic demonstrates basic usage of the cio client library.
func Example_basic() {
	// Create a new client with default configuration
	c, err := client.New()
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()

	// List GCS objects using full path
	objects, err := c.Storage().List(ctx, "gs://my-bucket/prefix/")
	if err != nil {
		log.Fatal(err)
	}

	for _, obj := range objects {
		fmt.Printf("Object: %s (Size: %d bytes)\n", obj.Name, obj.Size)
	}
}

// Example_withConfig demonstrates using a custom configuration.
func Example_withConfig() {
	// Create client with custom configuration
	c, err := client.New(client.Options{
		ConfigPath: "/path/to/config.yaml",
		ProjectID:  "my-project",
		Region:     "europe-west3",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()

	// List BigQuery tables
	tables, err := c.BigQuery().ListTables(ctx, "my-project", "my-dataset")
	if err != nil {
		log.Fatal(err)
	}

	for _, table := range tables {
		fmt.Printf("Table: %s\n", table.Name)
	}
}

// Example_aliasResolution demonstrates using alias-based paths.
func Example_aliasResolution() {
	// Create client (assumes ~/.config/cio/config.yaml exists with mappings)
	c, err := client.New()
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()

	// List using alias (if :am is mapped to gs://bucket/ in config)
	objects, err := c.Storage().List(ctx, ":am/2024/")
	if err != nil {
		log.Fatal(err)
	}

	for _, obj := range objects {
		fmt.Printf("Object: %s\n", obj.Name)
	}
}

// Example_uploadDownload demonstrates file upload and download.
func Example_uploadDownload() {
	c, err := client.New()
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()

	// Upload a file
	err = c.Storage().UploadFile(ctx, "/local/path/file.txt", "gs://bucket/remote/file.txt")
	if err != nil {
		log.Fatal(err)
	}

	// Download a file
	err = c.Storage().DownloadFile(ctx, "gs://bucket/remote/file.txt", "/local/path/downloaded.txt")
	if err != nil {
		log.Fatal(err)
	}
}

// Example_bigQueryOperations demonstrates BigQuery operations.
func Example_bigQueryOperations() {
	c, err := client.New(client.Options{
		ProjectID: "my-project",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()

	// List datasets
	datasets, err := c.BigQuery().ListDatasets(ctx, "my-project")
	if err != nil {
		log.Fatal(err)
	}

	for _, dataset := range datasets {
		fmt.Printf("Dataset: %s\n", dataset.Name)

		// List tables in each dataset
		tables, err := c.BigQuery().ListTables(ctx, "my-project", dataset.Name)
		if err != nil {
			continue
		}

		for _, table := range tables {
			fmt.Printf("  Table: %s\n", table.Name)
		}
	}
}

// Example_wildcards demonstrates wildcard pattern matching.
func Example_wildcards() {
	c, err := client.New()
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	ctx := context.Background()

	// List all .log files in a directory
	objects, err := c.Storage().ListWithPattern(ctx, "gs://bucket/logs/*.log")
	if err != nil {
		log.Fatal(err)
	}

	for _, obj := range objects {
		fmt.Printf("Log file: %s\n", obj.Name)
	}
}
