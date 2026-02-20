package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/storage"
	"github.com/thieso2/cio/apilog"
	"github.com/thieso2/cio/resolver"
)

const (
	// DefaultConcurrentUploads is the default number of concurrent upload operations
	DefaultConcurrentUploads = 50
)

// fileUpload represents a file to be uploaded
type fileUpload struct {
	localPath   string
	objectPath  string
	fullGCSPath string
}

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
	apilog.Logf("[GCS] Object.NewWriter(gs://%s/%s)", bucket, objectPath)
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

	fmt.Printf("Uploaded: %s → %s (%s)\n", localPath, formatter(fullGCSPath), FormatSize(fileInfo.Size()))
	return nil
}

// UploadDirectory uploads a directory recursively to GCS
func UploadDirectory(ctx context.Context, client *storage.Client, localPath, gcsPath string, verbose bool, formatter PathFormatter, maxWorkers int) error {
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

	// First pass: count total files
	var filesToUpload []fileUpload

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

		filesToUpload = append(filesToUpload, fileUpload{
			localPath:   path,
			objectPath:  objectPath,
			fullGCSPath: fullGCSPath,
		})

		return nil
	})

	if err != nil {
		return err
	}

	totalCount := len(filesToUpload)

	// Second pass: upload in parallel with progress counter
	return uploadFilesParallel(ctx, client, bucket, filesToUpload, totalCount, verbose, formatter, maxWorkers)
}

// uploadFilesParallel uploads files in parallel with controlled concurrency
func uploadFilesParallel(ctx context.Context, client *storage.Client, bucket string, filesToUpload []fileUpload, totalCount int, verbose bool, formatter PathFormatter, maxWorkers int) error {
	// Create a semaphore to limit concurrent uploads
	sem := make(chan struct{}, maxWorkers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	var completedCount int32

	// Channel for completed uploads (for progress tracking)
	type upload struct {
		localPath    string
		fullGCSPath  string
		bytesWritten int64
		err          error
	}
	uploads := make(chan upload, totalCount)

	// Start progress reporter goroutine
	done := make(chan struct{})
	go func() {
		for u := range uploads {
			count := atomic.AddInt32(&completedCount, 1)

			if u.err != nil {
				fmt.Printf("Failed %d/%d: %s - %v\n", count, totalCount, u.localPath, u.err)

				// Store first error
				mu.Lock()
				if firstErr == nil {
					firstErr = u.err
				}
				mu.Unlock()
			} else {
				size := FormatSize(u.bytesWritten)
				if verbose {
					fmt.Printf("Uploaded %d/%d: %s to %s (%s)\n", count, totalCount, u.localPath, formatter(u.fullGCSPath), size)
				} else {
					fmt.Printf("Uploaded %d/%d: %s → %s (%s)\n", count, totalCount, u.localPath, formatter(u.fullGCSPath), size)
				}
			}
		}
		close(done)
	}()

	// Upload files in parallel
	bkt := client.Bucket(bucket)
	for _, fu := range filesToUpload {
		wg.Add(1)

		// Acquire semaphore
		sem <- struct{}{}

		go func(fileUpload fileUpload) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore

			// Open local file
			file, err := os.Open(fileUpload.localPath)
			if err != nil {
				uploads <- upload{localPath: fileUpload.localPath, fullGCSPath: fileUpload.fullGCSPath, err: err}
				return
			}
			defer file.Close()

			// Stat for size
			info, err := file.Stat()
			if err != nil {
				uploads <- upload{localPath: fileUpload.localPath, fullGCSPath: fileUpload.fullGCSPath, err: err}
				return
			}

			// Create GCS object writer
			obj := bkt.Object(fileUpload.objectPath)
			apilog.Logf("[GCS] Object.NewWriter(%s)", fileUpload.fullGCSPath)
			writer := obj.NewWriter(ctx)

			// Copy file contents
			if _, err := io.Copy(writer, file); err != nil {
				writer.Close()
				uploads <- upload{localPath: fileUpload.localPath, fullGCSPath: fileUpload.fullGCSPath, err: err}
				return
			}

			// Close writer
			if err := writer.Close(); err != nil {
				uploads <- upload{localPath: fileUpload.localPath, fullGCSPath: fileUpload.fullGCSPath, err: err}
				return
			}

			// Send result to progress reporter
			uploads <- upload{localPath: fileUpload.localPath, fullGCSPath: fileUpload.fullGCSPath, bytesWritten: info.Size(), err: nil}
		}(fu)
	}

	// Wait for all uploads to complete
	wg.Wait()
	close(uploads)

	// Wait for progress reporter to finish
	<-done

	if firstErr != nil {
		return fmt.Errorf("upload failed: %w", firstErr)
	}

	if totalCount > 1 {
		fmt.Printf("\nTotal files uploaded: %d\n", totalCount)
	}
	return nil
}
