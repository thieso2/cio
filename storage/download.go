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
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const (
	// DefaultConcurrentDownloads is the default number of concurrent download operations
	DefaultConcurrentDownloads = 50
)

// DownloadOptions contains configuration for download operations
type DownloadOptions struct {
	// ParallelThreshold is the minimum file size (in bytes) to use parallel chunked download
	ParallelThreshold int64
	// ChunkSize is the size of each chunk for parallel downloads
	ChunkSize int64
	// MaxChunks is the maximum number of parallel chunks per file
	MaxChunks int
	// PreserveStructure preserves directory structure when downloading with wildcards
	PreserveStructure bool
}

// fileDownload represents a file to be downloaded
type fileDownload struct {
	objectName    string
	localFilePath string
	fullGCSPath   string
}

// chunkDownload represents a chunk of a file to be downloaded
type chunkDownload struct {
	offset int64
	length int64
	index  int
}

// DownloadFile downloads a single file from GCS to local filesystem
// Uses parallel chunked download for large files if opts is provided
func DownloadFile(ctx context.Context, client *storage.Client, bucket, object, localPath string, verbose bool, formatter PathFormatter, opts *DownloadOptions) error {
	if formatter == nil {
		formatter = DefaultPathFormatter
	}

	fullGCSPath := fmt.Sprintf("gs://%s/%s", bucket, object)

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

	// Get object attributes to check size
	obj := client.Bucket(bucket).Object(object)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get object attributes: %w", err)
	}

	// Decide whether to use parallel download
	useParallel := opts != nil && attrs.Size >= opts.ParallelThreshold

	if useParallel {
		if verbose {
			fmt.Printf("Downloading %s to %s (parallel mode, %d bytes)\n", formatter(fullGCSPath), localPath, attrs.Size)
		}
		return downloadFileParallel(ctx, client, bucket, object, localPath, attrs.Size, verbose, formatter, opts)
	}

	// Simple single-threaded download for small files
	if verbose {
		fmt.Printf("Downloading %s to %s\n", formatter(fullGCSPath), localPath)
	}

	// Track start time
	startTime := time.Now()

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()

	// Get GCS object reader
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

	// Calculate elapsed time and transfer rate
	elapsed := time.Since(startTime)
	if verbose {
		rate := float64(written) / elapsed.Seconds()
		fmt.Printf("Downloaded: %s → %s (%d bytes in %.2fs, %.2f MB/s)\n",
			formatter(fullGCSPath), localPath, written, elapsed.Seconds(), rate/1024/1024)
	} else {
		fmt.Printf("Downloaded: %s → %s (%d bytes)\n", formatter(fullGCSPath), localPath, written)
	}
	return nil
}

// downloadFileParallel downloads a file using parallel chunked download
func downloadFileParallel(ctx context.Context, client *storage.Client, bucket, object, localPath string, fileSize int64, verbose bool, formatter PathFormatter, opts *DownloadOptions) error {
	fullGCSPath := fmt.Sprintf("gs://%s/%s", bucket, object)

	// Calculate optimal number of chunks
	numChunks := int(fileSize / opts.ChunkSize)
	if fileSize%opts.ChunkSize != 0 {
		numChunks++
	}
	// Limit to MaxChunks
	if numChunks > opts.MaxChunks {
		numChunks = opts.MaxChunks
	}
	if numChunks < 1 {
		numChunks = 1
	}

	// Calculate actual chunk size
	actualChunkSize := fileSize / int64(numChunks)

	if verbose && numChunks > 1 {
		fmt.Printf("Using %d parallel chunks (%d bytes each)\n", numChunks, actualChunkSize)
	}

	// Track start time
	startTime := time.Now()

	// Create chunks
	chunks := make([]chunkDownload, numChunks)
	for i := 0; i < numChunks; i++ {
		offset := int64(i) * actualChunkSize
		length := actualChunkSize
		// Last chunk gets any remaining bytes
		if i == numChunks-1 {
			length = fileSize - offset
		}
		chunks[i] = chunkDownload{
			offset: offset,
			length: length,
			index:  i,
		}
	}

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()

	// Pre-allocate file size
	if err := file.Truncate(fileSize); err != nil {
		return fmt.Errorf("failed to allocate file: %w", err)
	}

	// Download chunks in parallel
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	var completedBytes int64
	obj := client.Bucket(bucket).Object(object)

	// Progress ticker for verbose mode
	var ticker *time.Ticker
	var done chan struct{}
	if verbose {
		ticker = time.NewTicker(2 * time.Second)
		done = make(chan struct{})
		go func() {
			lastProgress := int64(0)
			for {
				select {
				case <-ticker.C:
					downloaded := atomic.LoadInt64(&completedBytes)
					// Only show progress if it has changed
					if downloaded > lastProgress {
						percent := float64(downloaded) / float64(fileSize) * 100
						fmt.Printf("Progress: %.1f%% (%d/%d bytes)\n", percent, downloaded, fileSize)
						lastProgress = downloaded
					}
				case <-done:
					return
				}
			}
		}()
	}

	// Download each chunk
	for _, chunk := range chunks {
		wg.Add(1)
		go func(c chunkDownload) {
			defer wg.Done()

			// Create range reader for this chunk
			reader, err := obj.NewRangeReader(ctx, c.offset, c.length)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("failed to create range reader for chunk %d: %w", c.index, err)
				}
				mu.Unlock()
				return
			}
			defer reader.Close()

			// Read chunk data
			buf := make([]byte, c.length)
			n, err := io.ReadFull(reader, buf)
			if err != nil && err != io.ErrUnexpectedEOF {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("failed to read chunk %d: %w", c.index, err)
				}
				mu.Unlock()
				return
			}

			// Write to file at correct offset
			mu.Lock()
			_, err = file.WriteAt(buf[:n], c.offset)
			mu.Unlock()
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("failed to write chunk %d: %w", c.index, err)
				}
				mu.Unlock()
				return
			}

			// Update progress
			atomic.AddInt64(&completedBytes, int64(n))
		}(chunk)
	}

	// Wait for all chunks to complete
	wg.Wait()

	// Stop progress ticker
	if verbose && ticker != nil {
		ticker.Stop()
		close(done)
	}

	if firstErr != nil {
		return firstErr
	}

	// Calculate elapsed time and transfer rate
	elapsed := time.Since(startTime)
	if verbose {
		rate := float64(fileSize) / elapsed.Seconds()
		if numChunks > 1 {
			fmt.Printf("Downloaded: %s → %s (%d bytes, %d chunks in %.2fs, %.2f MB/s)\n",
				formatter(fullGCSPath), localPath, fileSize, numChunks, elapsed.Seconds(), rate/1024/1024)
		} else {
			fmt.Printf("Downloaded: %s → %s (%d bytes in %.2fs, %.2f MB/s)\n",
				formatter(fullGCSPath), localPath, fileSize, elapsed.Seconds(), rate/1024/1024)
		}
	} else {
		if numChunks > 1 {
			fmt.Printf("Downloaded: %s → %s (%d bytes, %d chunks)\n", formatter(fullGCSPath), localPath, fileSize, numChunks)
		} else {
			fmt.Printf("Downloaded: %s → %s (%d bytes)\n", formatter(fullGCSPath), localPath, fileSize)
		}
	}
	return nil
}

// DownloadDirectory downloads all objects with a given prefix recursively
func DownloadDirectory(ctx context.Context, client *storage.Client, bucket, prefix, localPath string, verbose bool, formatter PathFormatter, maxWorkers int, opts *DownloadOptions) error {
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
	return downloadFilesParallel(ctx, client, bucket, filesToDownload, totalCount, verbose, formatter, maxWorkers, opts)
}

// DownloadWithPattern downloads all objects matching a wildcard pattern
func DownloadWithPattern(ctx context.Context, client *storage.Client, bucket, pattern, localPath string, verbose bool, formatter PathFormatter, maxWorkers int, opts *DownloadOptions) error {
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

		// Calculate local file path
		var localFilePath string
		if opts != nil && opts.PreserveStructure {
			// Preserve directory structure (like cp -r)
			relPath := strings.TrimPrefix(attrs.Name, prefix)
			if relPath == "" {
				continue
			}
			localFilePath = filepath.Join(localPath, filepath.FromSlash(relPath))
		} else {
			// Flatten directory structure (just use filename)
			filename := filepath.Base(attrs.Name)
			localFilePath = filepath.Join(localPath, filename)
		}
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
	return downloadFilesParallel(ctx, client, bucket, filesToDownload, totalCount, verbose, formatter, maxWorkers, opts)
}

// downloadFilesParallel downloads files in parallel with controlled concurrency
// For now, this downloads multiple files in parallel (outer parallelism)
// Future enhancement: Use DownloadFile for each file to get parallel chunked downloads (inner parallelism)
func downloadFilesParallel(ctx context.Context, client *storage.Client, bucket string, filesToDownload []fileDownload, totalCount int, verbose bool, formatter PathFormatter, maxWorkers int, opts *DownloadOptions) error {
	// Track start time for overall transfer rate
	startTime := time.Now()
	var totalBytes int64

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
				// Track total bytes downloaded
				atomic.AddInt64(&totalBytes, d.bytesWritten)

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

	// Calculate elapsed time and overall transfer rate
	elapsed := time.Since(startTime)
	if totalCount > 1 {
		if verbose {
			bytes := atomic.LoadInt64(&totalBytes)
			rate := float64(bytes) / elapsed.Seconds()
			fmt.Printf("\nTotal files downloaded: %d (%d bytes in %.2fs, %.2f MB/s)\n",
				totalCount, bytes, elapsed.Seconds(), rate/1024/1024)
		} else {
			fmt.Printf("\nTotal files downloaded: %d\n", totalCount)
		}
	}
	return nil
}
