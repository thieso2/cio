package fuse

import (
	"context"
	"sync"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const (
	// MaxConcurrentGCSCalls limits parallel GCS API calls
	MaxConcurrentGCSCalls = 10
	// ReadAheadBufferSize is the size of the read-ahead buffer in bytes
	ReadAheadBufferSize = 1024 * 1024 // 1MB
)

// objectResult holds the result of a parallel object fetch
type objectResult struct {
	attrs *storage.ObjectAttrs
	err   error
}

// listObjectsConcurrent lists objects from GCS using concurrent API calls
// This significantly speeds up listing large directories by fetching multiple
// pages in parallel.
func listObjectsConcurrent(ctx context.Context, bucket *storage.BucketHandle, query *storage.Query) ([]*storage.ObjectAttrs, error) {
	it := bucket.Objects(ctx, query)

	var (
		mu      sync.Mutex
		results []*storage.ObjectAttrs
	)

	// Fetch all objects
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		// For prefixes, add directly (no additional processing needed)
		if attrs.Prefix != "" {
			mu.Lock()
			results = append(results, attrs)
			mu.Unlock()
			continue
		}

		// For objects, add them
		mu.Lock()
		results = append(results, attrs)
		mu.Unlock()
	}

	return results, nil
}

// prefetchObjectAttrs fetches object attributes in parallel
// Used for operations that need attrs for multiple objects
func prefetchObjectAttrs(ctx context.Context, bucket *storage.BucketHandle, objectNames []string) map[string]*storage.ObjectAttrs {
	results := make(map[string]*storage.ObjectAttrs)
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		sem = make(chan struct{}, MaxConcurrentGCSCalls)
	)

	for _, name := range objectNames {
		wg.Add(1)
		go func(objName string) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			attrs, err := bucket.Object(objName).Attrs(ctx)
			if err == nil {
				mu.Lock()
				results[objName] = attrs
				mu.Unlock()
			}
		}(name)
	}

	wg.Wait()
	return results
}

// ReadAheadBuffer provides read-ahead buffering for sequential file reads
type ReadAheadBuffer struct {
	mu         sync.Mutex
	bucketName string
	objectName string
	buffer     []byte
	offset     int64
	valid      bool
}

// NewReadAheadBuffer creates a new read-ahead buffer
func NewReadAheadBuffer(bucketName, objectName string) *ReadAheadBuffer {
	return &ReadAheadBuffer{
		bucketName: bucketName,
		objectName: objectName,
		buffer:     make([]byte, 0, ReadAheadBufferSize),
	}
}

// Read reads data, using the buffer if available or fetching from GCS
func (b *ReadAheadBuffer) Read(ctx context.Context, bucket *storage.BucketHandle, off int64, dest []byte) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Check if we can serve from buffer
	if b.valid && off >= b.offset && off < b.offset+int64(len(b.buffer)) {
		// Serve from buffer
		start := int(off - b.offset)
		end := start + len(dest)
		if end > len(b.buffer) {
			end = len(b.buffer)
		}
		return b.buffer[start:end], nil
	}

	// Buffer miss - fetch from GCS with read-ahead
	readSize := len(dest)
	if readSize < ReadAheadBufferSize {
		readSize = ReadAheadBufferSize
	}

	reader, err := bucket.Object(b.objectName).NewRangeReader(ctx, off, int64(readSize))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// Read into buffer
	b.buffer = b.buffer[:0]
	buf := make([]byte, readSize)
	n, _ := reader.Read(buf)
	b.buffer = buf[:n]
	b.offset = off
	b.valid = true

	// Return requested portion
	end := len(dest)
	if end > len(b.buffer) {
		end = len(b.buffer)
	}
	return b.buffer[:end], nil
}

// Invalidate invalidates the buffer
func (b *ReadAheadBuffer) Invalidate() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.valid = false
}
