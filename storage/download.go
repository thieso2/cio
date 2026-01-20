package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const (
	// DefaultConcurrentDownloads is the default number of concurrent download operations
	DefaultConcurrentDownloads = 50
)

// fileDownload represents a file to be downloaded
type fileDownload struct {
	objectName    string
	localFilePath string
	fullGCSPath   string
}

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
func DownloadDirectory(ctx context.Context, client *storage.Client, bucket, prefix, localPath string, verbose bool, formatter PathFormatter, maxWorkers int) error {
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

	// First pass: collect all objects to download
	var filesToDownload []fileDownload

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

		filesToDownload = append(filesToDownload, fileDownload{
			objectName:    attrs.Name,
			localFilePath: localFilePath,
			fullGCSPath:   fullGCSPath,
		})
	}

	totalCount := len(filesToDownload)
	if totalCount == 0 {
		return fmt.Errorf("no objects found with prefix gs://%s/%s", bucket, prefix)
	}

	// Second pass: download in parallel with progress counter
	return downloadFilesParallel(ctx, client, bucket, filesToDownload, totalCount, verbose, formatter, maxWorkers)
}

// DownloadWithPattern downloads all objects matching a wildcard pattern
func DownloadWithPattern(ctx context.Context, client *storage.Client, bucket, pattern, localPath string, verbose bool, formatter PathFormatter, maxWorkers int) error {
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

	// First pass: collect all matching objects
	var filesToDownload []fileDownload

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

		filesToDownload = append(filesToDownload, fileDownload{
			objectName:    attrs.Name,
			localFilePath: localFilePath,
			fullGCSPath:   fullGCSPath,
		})
	}

	totalCount := len(filesToDownload)
	if totalCount == 0 {
		return fmt.Errorf("no objects found matching pattern: %s", pattern)
	}

	// Second pass: download in parallel with progress counter
	return downloadFilesParallel(ctx, client, bucket, filesToDownload, totalCount, verbose, formatter, maxWorkers)
}

// downloadFilesParallel downloads files in parallel with controlled concurrency
func downloadFilesParallel(ctx context.Context, client *storage.Client, bucket string, filesToDownload []fileDownload, totalCount int, verbose bool, formatter PathFormatter, maxWorkers int) error {
	// Create a semaphore to limit concurrent downloads
	sem := make(chan struct{}, maxWorkers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	var completedCount int32

	// Channel for completed downloads (for progress tracking)
	type download struct {
		fullGCSPath   string
		localFilePath string
		bytesWritten  int64
		err           error
	}
	downloads := make(chan download, totalCount)

	// Start progress reporter goroutine
	done := make(chan struct{})
	go func() {
		for d := range downloads {
			count := atomic.AddInt32(&completedCount, 1)

			if d.err != nil {
				fmt.Printf("Failed %d/%d: %s - %v\n", count, totalCount, formatter(d.fullGCSPath), d.err)

				// Store first error
				mu.Lock()
				if firstErr == nil {
					firstErr = d.err
				}
				mu.Unlock()
			} else {
				if verbose {
					fmt.Printf("Downloaded %d/%d: %s to %s (%d bytes)\n", count, totalCount, formatter(d.fullGCSPath), d.localFilePath, d.bytesWritten)
				} else {
					fmt.Printf("Downloaded %d/%d: %s → %s (%d bytes)\n", count, totalCount, formatter(d.fullGCSPath), d.localFilePath, d.bytesWritten)
				}
			}
		}
		close(done)
	}()

	// Download files in parallel
	bkt := client.Bucket(bucket)
	for _, fd := range filesToDownload {
		wg.Add(1)

		// Acquire semaphore
		sem <- struct{}{}

		go func(fileDownload fileDownload) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore

			// Ensure parent directory exists
			dir := filepath.Dir(fileDownload.localFilePath)
			if err := os.MkdirAll(dir, 0755); err != nil {
				downloads <- download{fullGCSPath: fileDownload.fullGCSPath, localFilePath: fileDownload.localFilePath, err: err}
				return
			}

			// Create local file
			file, err := os.Create(fileDownload.localFilePath)
			if err != nil {
				downloads <- download{fullGCSPath: fileDownload.fullGCSPath, localFilePath: fileDownload.localFilePath, err: err}
				return
			}
			defer file.Close()

			// Get GCS object reader
			obj := bkt.Object(fileDownload.objectName)
			reader, err := obj.NewReader(ctx)
			if err != nil {
				downloads <- download{fullGCSPath: fileDownload.fullGCSPath, localFilePath: fileDownload.localFilePath, err: err}
				return
			}
			defer reader.Close()

			// Copy contents
			written, err := io.Copy(file, reader)
			if err != nil {
				downloads <- download{fullGCSPath: fileDownload.fullGCSPath, localFilePath: fileDownload.localFilePath, err: err}
				return
			}

			// Send result to progress reporter
			downloads <- download{fullGCSPath: fileDownload.fullGCSPath, localFilePath: fileDownload.localFilePath, bytesWritten: written, err: nil}
		}(fd)
	}

	// Wait for all downloads to complete
	wg.Wait()
	close(downloads)

	// Wait for progress reporter to finish
	<-done

	if firstErr != nil {
		return fmt.Errorf("download failed: %w", firstErr)
	}

	if totalCount > 1 {
		fmt.Printf("\nTotal files downloaded: %d\n", totalCount)
	}
	return nil
}
