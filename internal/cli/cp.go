package cli

import (
	"context"
	"fmt"
	"os"
	"strings"

	gcs "cloud.google.com/go/storage"
	"github.com/spf13/cobra"
	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/storage"
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
  - Directory structure preservation with -r flag

Examples:
  # Upload local file to GCS
  cio cp data.csv :am/2024/

  # Upload with alias expansion
  cio cp report.pdf :am/reports/2024/

  # Download from GCS to local
  cio cp :am/2024/data.csv ./downloads/

  # Download with wildcard pattern (flattens directory structure)
  cio cp ':am/logs/*.log' ./local-logs/

  # Download with wildcard pattern preserving structure (like cp -r)
  cio cp -r ':am/logs/*' ./local-logs/

  # Recursive upload
  cio cp -r ./logs/ :am/logs/2024/

  # Recursive download
  cio cp -r :am/logs/2024/ ./local-logs/`,
	Args: cobra.MinimumNArgs(2),
	RunE: runCp,
}

func init() {
	rootCmd.AddCommand(cpCmd)
	cpCmd.Flags().BoolVarP(&cpRecursive, "recursive", "r", false, "copy directories recursively")
}

func runCp(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	sources := args[:len(args)-1]
	destination := args[len(args)-1]

	r := resolver.Create(cfg)

	// isCloudPath returns true for gs://, bq://, iam:// and alias (:) paths
	isCloudPath := func(p string) bool {
		return resolver.IsGCSPath(p) || resolver.IsBQPath(p) || resolver.IsIAMPath(p) || strings.HasPrefix(p, ":")
	}

	// Resolve destination once
	destIsLocal := !isCloudPath(destination)
	var destPath string
	var destWasAlias bool
	if !destIsLocal {
		if resolver.IsGCSPath(destination) {
			destPath = destination
		} else {
			var err error
			destPath, err = r.Resolve(destination)
			if err != nil {
				return fmt.Errorf("failed to resolve destination: %w", err)
			}
			destWasAlias = true
		}
	} else {
		destPath = destination
	}

	// Get GCS client (needed for any GCS operation)
	client, err := storage.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}

	for _, source := range sources {
		sourceIsLocal := !isCloudPath(source)

		var sourcePath string
		var sourceWasAlias bool
		if !sourceIsLocal {
			if resolver.IsGCSPath(source) {
				sourcePath = source
			} else {
				sourcePath, err = r.Resolve(source)
				if err != nil {
					return fmt.Errorf("failed to resolve source %q: %w", source, err)
				}
				sourceWasAlias = true
			}
		} else {
			sourcePath = source
		}

		var copyErr error
		if sourceIsLocal && !destIsLocal {
			copyErr = uploadPath(ctx, client, r, sourcePath, destPath, destWasAlias)
		} else if !sourceIsLocal && destIsLocal {
			copyErr = downloadPath(ctx, client, r, sourcePath, destPath, sourceWasAlias)
		} else if !sourceIsLocal && !destIsLocal {
			return fmt.Errorf("GCS to GCS copy not yet implemented")
		} else {
			return fmt.Errorf("use system 'cp' command for local to local copy")
		}
		if copyErr != nil {
			return copyErr
		}
	}
	return nil
}

func uploadPath(ctx context.Context, client *gcs.Client, r *resolver.Resolver, localPath, gcsPath string, destWasAlias bool) error {
	// Check if source exists
	fileInfo, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("cannot access %q: %w", localPath, err)
	}

	// Create path formatter - only reverse-map if destination was an alias
	var formatter storage.PathFormatter
	if destWasAlias {
		formatter = r.ReverseResolve
	} else {
		formatter = func(path string) string { return path }
	}

	if fileInfo.IsDir() {
		if !cpRecursive {
			return fmt.Errorf("%q is a directory (use -r to copy recursively)", localPath)
		}
		return storage.UploadDirectory(ctx, client, localPath, gcsPath, verbose, formatter, GetParallelism())
	}

	return storage.UploadFile(ctx, client, localPath, gcsPath, verbose, formatter)
}

func downloadPath(ctx context.Context, client *gcs.Client, r *resolver.Resolver, gcsPath, localPath string, sourceWasAlias bool) error {
	// Parse GCS path
	bucket, object, err := resolver.ParseGCSPath(gcsPath)
	if err != nil {
		return err
	}

	// Create path formatter - only reverse-map if source was an alias
	var formatter storage.PathFormatter
	if sourceWasAlias {
		formatter = r.ReverseResolve
	} else {
		formatter = func(path string) string { return path }
	}

	// Create download options from config
	// Respect parallelism flag as a limit for max chunks
	maxChunks := cfg.Download.MaxChunks
	parallelism := GetParallelism()
	if parallelism < maxChunks {
		maxChunks = parallelism
	}

	opts := &storage.DownloadOptions{
		ParallelThreshold: cfg.Download.ParallelThreshold,
		ChunkSize:         cfg.Download.ChunkSize,
		MaxChunks:         maxChunks,
		PreserveStructure: cpRecursive, // Preserve directory structure when -r flag is used
	}

	// Check if path contains wildcards
	if resolver.HasWildcard(object) {
		return storage.DownloadWithPattern(ctx, client, bucket, object, localPath, verbose, formatter, GetParallelism(), opts)
	}

	// Check if this is a directory (ends with / or no object specified)
	if object == "" || object[len(object)-1] == '/' {
		if !cpRecursive {
			return fmt.Errorf("%q appears to be a directory (use -r to copy recursively)", gcsPath)
		}
		return storage.DownloadDirectory(ctx, client, bucket, object, localPath, verbose, formatter, GetParallelism(), opts)
	}

	return storage.DownloadFile(ctx, client, bucket, object, localPath, verbose, formatter, opts)
}
