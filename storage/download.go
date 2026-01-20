package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// DownloadFile downloads a single file from GCS to local filesystem
func DownloadFile(ctx context.Context, client *storage.Client, bucket, object, localPath string, verbose bool, formatter PathFormatter) error {
	if formatter == nil {
		formatter = DefaultPathFormatter
	}

	fullGCSPath := fmt.Sprintf("gs://%s/%s", bucket, object)

	if verbose {
		fmt.Printf("Downloading %s to %s\n", formatter(fullGCSPath), localPath)
	}

	// If localPath is a directory, append the object's filename
	fileInfo, err := os.Stat(localPath)
	if err == nil && fileInfo.IsDir() {
		filename := filepath.Base(object)
		localPath = filepath.Join(localPath, filename)
	}

	// Ensure parent directory exists
	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()

	// Get GCS object reader
	obj := client.Bucket(bucket).Object(object)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return fmt.Errorf("failed to read from GCS: %w", err)
	}
	defer reader.Close()

	// Copy contents to local file
	written, err := io.Copy(file, reader)
	if err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}

	fmt.Printf("Downloaded: %s → %s (%d bytes)\n", formatter(fullGCSPath), localPath, written)
	return nil
}

// DownloadDirectory downloads all objects with a given prefix recursively
func DownloadDirectory(ctx context.Context, client *storage.Client, bucket, prefix, localPath string, verbose bool, formatter PathFormatter) error {
	if formatter == nil {
		formatter = DefaultPathFormatter
	}
	// Ensure local path exists
	if err := os.MkdirAll(localPath, 0755); err != nil {
		return fmt.Errorf("failed to create local directory: %w", err)
	}

	// List all objects with the prefix
	bkt := client.Bucket(bucket)
	query := &storage.Query{
		Prefix: prefix,
	}

	downloadCount := 0
	it := bkt.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		// Skip directory markers (objects ending with /)
		if strings.HasSuffix(attrs.Name, "/") {
			continue
		}

		// Calculate local file path
		relPath := strings.TrimPrefix(attrs.Name, prefix)
		if relPath == "" {
			continue
		}
		localFilePath := filepath.Join(localPath, filepath.FromSlash(relPath))
		fullGCSPath := fmt.Sprintf("gs://%s/%s", bucket, attrs.Name)

		if verbose {
			fmt.Printf("Downloading %s to %s\n", formatter(fullGCSPath), localFilePath)
		}

		// Ensure parent directory exists
		dir := filepath.Dir(localFilePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}

		// Create local file
		file, err := os.Create(localFilePath)
		if err != nil {
			return fmt.Errorf("failed to create %s: %w", localFilePath, err)
		}

		// Get GCS object reader
		obj := bkt.Object(attrs.Name)
		reader, err := obj.NewReader(ctx)
		if err != nil {
			file.Close()
			return fmt.Errorf("failed to read %s: %w", formatter(fullGCSPath), err)
		}

		// Copy contents
		written, err := io.Copy(file, reader)
		reader.Close()
		file.Close()

		if err != nil {
			return fmt.Errorf("failed to download %s: %w", attrs.Name, err)
		}

		downloadCount++
		if !verbose {
			fmt.Printf("Downloaded: %s → %s (%d bytes)\n", formatter(fullGCSPath), localFilePath, written)
		}
	}

	if downloadCount == 0 {
		return fmt.Errorf("no objects found with prefix gs://%s/%s", bucket, prefix)
	}

	fmt.Printf("\nTotal files downloaded: %d\n", downloadCount)
	return nil
}

// DownloadWithPattern downloads all objects matching a wildcard pattern
func DownloadWithPattern(ctx context.Context, client *storage.Client, bucket, pattern, localPath string, verbose bool, formatter PathFormatter) error {
	if formatter == nil {
		formatter = DefaultPathFormatter
	}
	// Ensure local path exists
	if err := os.MkdirAll(localPath, 0755); err != nil {
		return fmt.Errorf("failed to create local directory: %w", err)
	}

	// Extract prefix and wildcard pattern
	prefix, wildcardPattern := splitPattern(pattern)

	// List all objects with the prefix
	bkt := client.Bucket(bucket)
	query := &storage.Query{
		Prefix: prefix,
	}

	downloadCount := 0
	it := bkt.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		// Skip directory markers
		if strings.HasSuffix(attrs.Name, "/") {
			continue
		}

		// Check if object matches the pattern
		if !matchesPattern(attrs.Name, wildcardPattern) {
			continue
		}

		// Calculate local file path (use just the filename)
		filename := filepath.Base(attrs.Name)
		localFilePath := filepath.Join(localPath, filename)
		fullGCSPath := fmt.Sprintf("gs://%s/%s", bucket, attrs.Name)

		if verbose {
			fmt.Printf("Downloading %s to %s\n", formatter(fullGCSPath), localFilePath)
		}

		// Create local file
		file, err := os.Create(localFilePath)
		if err != nil {
			return fmt.Errorf("failed to create %s: %w", localFilePath, err)
		}

		// Get GCS object reader
		obj := bkt.Object(attrs.Name)
		reader, err := obj.NewReader(ctx)
		if err != nil {
			file.Close()
			return fmt.Errorf("failed to read %s: %w", formatter(fullGCSPath), err)
		}

		// Copy contents
		written, err := io.Copy(file, reader)
		reader.Close()
		file.Close()

		if err != nil {
			return fmt.Errorf("failed to download %s: %w", attrs.Name, err)
		}

		downloadCount++
		if !verbose {
			fmt.Printf("Downloaded: %s → %s (%d bytes)\n", formatter(fullGCSPath), localFilePath, written)
		}
	}

	if downloadCount == 0 {
		return fmt.Errorf("no objects found matching pattern: %s", pattern)
	}

	fmt.Printf("\nTotal files downloaded: %d\n", downloadCount)
	return nil
}
