package cli

import (
	"context"
	"fmt"
	"os"

	gcs "cloud.google.com/go/storage"
	"github.com/spf13/cobra"
	"github.com/thieso2/cio/internal/resolver"
	"github.com/thieso2/cio/internal/storage"
)

var (
	cpRecursive bool
)

// cpCmd represents the cp command
var cpCmd = &cobra.Command{
	Use:   "cp <source> <destination>",
	Short: "Copy files between local and GCS",
	Long: `Copy files between local filesystem and Google Cloud Storage.

Supports:
  - Local to GCS: cio cp file.txt :am/path/
  - GCS to local: cio cp :am/path/file.txt ./local/
  - Recursive directory copy with -r flag
  - Wildcard patterns: cio cp ':am/logs/*.log' ./local/

Examples:
  # Upload local file to GCS
  cio cp data.csv :am/2024/

  # Upload with alias expansion
  cio cp report.pdf :am/reports/2024/

  # Download from GCS to local
  cio cp :am/2024/data.csv ./downloads/

  # Download with wildcard pattern
  cio cp ':am/logs/*.log' ./local-logs/

  # Recursive upload
  cio cp -r ./logs/ :am/logs/2024/

  # Recursive download
  cio cp -r :am/logs/2024/ ./local-logs/`,
	Args: cobra.ExactArgs(2),
	RunE: runCp,
}

func init() {
	rootCmd.AddCommand(cpCmd)
	cpCmd.Flags().BoolVarP(&cpRecursive, "recursive", "r", false, "copy directories recursively")
}

func runCp(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	source := args[0]
	destination := args[1]

	// Determine copy direction
	sourceIsLocal := !resolver.IsGCSPath(source)
	destIsLocal := !resolver.IsGCSPath(destination)

	// Resolve aliases
	r := resolver.Create(cfg)

	var sourcePath, destPath string
	var err error

	if !sourceIsLocal {
		sourcePath, err = r.Resolve(source)
		if err != nil {
			return fmt.Errorf("failed to resolve source: %w", err)
		}
	} else {
		sourcePath = source
	}

	if !destIsLocal {
		destPath, err = r.Resolve(destination)
		if err != nil {
			return fmt.Errorf("failed to resolve destination: %w", err)
		}
	} else {
		destPath = destination
	}

	// Get GCS client
	client, err := storage.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}

	// Execute copy based on direction
	if sourceIsLocal && !destIsLocal {
		// Local -> GCS (upload)
		return uploadPath(ctx, client, sourcePath, destPath)
	} else if !sourceIsLocal && destIsLocal {
		// GCS -> Local (download)
		return downloadPath(ctx, client, sourcePath, destPath)
	} else if !sourceIsLocal && !destIsLocal {
		// GCS -> GCS (not yet implemented)
		return fmt.Errorf("GCS to GCS copy not yet implemented")
	} else {
		// Local -> Local (use system cp instead)
		return fmt.Errorf("use system 'cp' command for local to local copy")
	}
}

func uploadPath(ctx context.Context, client *gcs.Client, localPath, gcsPath string) error {
	// Check if source exists
	fileInfo, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("cannot access %q: %w", localPath, err)
	}

	// Create path formatter using resolver
	r := resolver.Create(cfg)
	formatter := r.ReverseResolve

	if fileInfo.IsDir() {
		if !cpRecursive {
			return fmt.Errorf("%q is a directory (use -r to copy recursively)", localPath)
		}
		return storage.UploadDirectory(ctx, client, localPath, gcsPath, verbose, formatter)
	}

	return storage.UploadFile(ctx, client, localPath, gcsPath, verbose, formatter)
}

func downloadPath(ctx context.Context, client *gcs.Client, gcsPath, localPath string) error {
	// Parse GCS path
	bucket, object, err := resolver.ParseGCSPath(gcsPath)
	if err != nil {
		return err
	}

	// Create path formatter using resolver
	r := resolver.Create(cfg)
	formatter := r.ReverseResolve

	// Check if path contains wildcards
	if resolver.HasWildcard(object) {
		return storage.DownloadWithPattern(ctx, client, bucket, object, localPath, verbose, formatter)
	}

	// Check if this is a directory (ends with / or no object specified)
	if object == "" || object[len(object)-1] == '/' {
		if !cpRecursive {
			return fmt.Errorf("%q appears to be a directory (use -r to copy recursively)", gcsPath)
		}
		return storage.DownloadDirectory(ctx, client, bucket, object, localPath, verbose, formatter)
	}

	return storage.DownloadFile(ctx, client, bucket, object, localPath, verbose, formatter)
}
