package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
	"github.com/thieso2/cio/resolver"
)

// PathFormatter is a function that formats GCS paths for display
// It can convert gs:// paths to alias paths for better readability
type PathFormatter func(gcsPath string) string

// DefaultPathFormatter returns paths unchanged
func DefaultPathFormatter(gcsPath string) string {
	return gcsPath
}

// UploadFile uploads a single file to GCS
func UploadFile(ctx context.Context, client *storage.Client, localPath, gcsPath string, verbose bool, formatter PathFormatter) error {
	if formatter == nil {
		formatter = DefaultPathFormatter
	}

	// Parse GCS path
	bucket, objectPath, err := resolver.ParseGCSPath(gcsPath)
	if err != nil {
		return err
	}

	// If gcsPath ends with /, append the filename
	if objectPath == "" || objectPath[len(objectPath)-1] == '/' {
		filename := filepath.Base(localPath)
		objectPath = objectPath + filename
	}

	fullGCSPath := fmt.Sprintf("gs://%s/%s", bucket, objectPath)

	// Open local file
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open local file: %w", err)
	}
	defer file.Close()

	// Get file info for size
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	if verbose {
		fmt.Printf("Uploading %s to %s (%d bytes)\n", localPath, formatter(fullGCSPath), fileInfo.Size())
	}

	// Create GCS object writer
	obj := client.Bucket(bucket).Object(objectPath)
	writer := obj.NewWriter(ctx)

	// Copy file contents to GCS
	if _, err := io.Copy(writer, file); err != nil {
		writer.Close()
		return fmt.Errorf("failed to upload file: %w", err)
	}

	// Close writer (this commits the upload)
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	fmt.Printf("Uploaded: %s → %s\n", localPath, formatter(fullGCSPath))
	return nil
}

// UploadDirectory uploads a directory recursively to GCS
func UploadDirectory(ctx context.Context, client *storage.Client, localPath, gcsPath string, verbose bool, formatter PathFormatter) error {
	if formatter == nil {
		formatter = DefaultPathFormatter
	}

	// Parse GCS path
	bucket, basePrefix, err := resolver.ParseGCSPath(gcsPath)
	if err != nil {
		return err
	}

	// Ensure base prefix ends with /
	if basePrefix != "" && basePrefix[len(basePrefix)-1] != '/' {
		basePrefix = basePrefix + "/"
	}

	// Get the directory name
	dirName := filepath.Base(localPath)

	// Walk through the directory
	uploadCount := 0
	err = filepath.Walk(localPath, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		// Skip directories themselves
		if info.IsDir() {
			return nil
		}

		// Calculate relative path
		relPath, err := filepath.Rel(localPath, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %w", err)
		}

		// Convert to GCS path (use forward slashes)
		relPath = filepath.ToSlash(relPath)
		objectPath := basePrefix + dirName + "/" + relPath
		fullGCSPath := fmt.Sprintf("gs://%s/%s", bucket, objectPath)

		if verbose {
			fmt.Printf("Uploading %s to %s\n", path, formatter(fullGCSPath))
		}

		// Open local file
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open %s: %w", path, err)
		}
		defer file.Close()

		// Create GCS object writer
		obj := client.Bucket(bucket).Object(objectPath)
		writer := obj.NewWriter(ctx)

		// Copy file contents
		if _, err := io.Copy(writer, file); err != nil {
			writer.Close()
			return fmt.Errorf("failed to upload %s: %w", path, err)
		}

		// Close writer
		if err := writer.Close(); err != nil {
			return fmt.Errorf("failed to close writer for %s: %w", path, err)
		}

		uploadCount++
		if !verbose {
			fmt.Printf("Uploaded: %s → %s\n", path, formatter(fullGCSPath))
		}

		return nil
	})

	if err != nil {
		return err
	}

	fmt.Printf("\nTotal files uploaded: %d\n", uploadCount)
	return nil
}
