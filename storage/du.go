package storage

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/thieso2/cio/apilog"
	"google.golang.org/api/iterator"
)

// DUOptions configures disk usage calculation behavior.
type DUOptions struct {
	// Workers is the number of parallel goroutines for subdirectory summation (default 8).
	Workers int
}

// DefaultDUOptions returns sensible defaults.
func DefaultDUOptions() *DUOptions {
	return &DUOptions{Workers: 8}
}

// DUEntry holds the size of a single immediate subdirectory.
type DUEntry struct {
	Path string
	Size int64
}

// DUResult holds the output of a disk usage calculation.
type DUResult struct {
	// Entries contains one entry per immediate subdirectory, sorted by path.
	Entries []DUEntry
	// RootPath is the queried root gs:// path.
	RootPath string
	// Total is the grand total across all entries plus any root-level files.
	Total int64
}

// DiskUsage calculates disk usage for a GCS prefix, parallelizing by
// immediate subdirectory (the natural unit for du-style output).
//
// Algorithm:
//  1. Shallow-list the prefix to discover immediate children.
//  2. Root-level files are counted directly from the listing (their sizes are
//     already in the ObjectInfo struct, so no extra API calls are needed).
//  3. Each subdirectory is summed by a goroutine that does a recursive listing
//     with SetAttrSelection(["Name","Size"]) to minimise payload and cost.
//  4. Results are collected, sorted by path, and returned.
func DiskUsage(ctx context.Context, bucket, prefix string, opts *DUOptions) (*DUResult, error) {
	if opts == nil {
		opts = DefaultDUOptions()
	}
	if opts.Workers <= 0 {
		opts.Workers = 8
	}

	client, err := GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	rootPath := fmt.Sprintf("gs://%s/%s", bucket, prefix)

	// Step 1: shallow list to discover immediate children.
	entries, err := List(ctx, bucket, prefix, &ListOptions{
		Recursive: false,
		Delimiter: "/",
	})
	if err != nil {
		return nil, err
	}

	// If there are no results at all and a prefix was specified, the path may
	// be a single object rather than a "directory". Handle it gracefully.
	if len(entries) == 0 && prefix != "" {
		obj := client.Bucket(bucket).Object(prefix)
		apilog.Logf("[GCS] Object.Attrs(gs://%s/%s) [du single-file probe]", bucket, prefix)
		attrs, attrErr := obj.Attrs(ctx)
		if attrErr == nil {
			return &DUResult{
				RootPath: rootPath,
				Total:    attrs.Size,
			}, nil
		}
		// Object not found either – return a zero result.
		return &DUResult{RootPath: rootPath}, nil
	}

	// Step 2: separate subdirectories from root-level files.
	var subdirPrefixes []string
	var rootFileTotal int64
	for _, e := range entries {
		if e.IsPrefix {
			// Strip gs://bucket/ to get the raw GCS prefix string.
			subPrefix := strings.TrimPrefix(e.Path, "gs://"+bucket+"/")
			subdirPrefixes = append(subdirPrefixes, subPrefix)
		} else {
			rootFileTotal += e.Size
		}
	}

	// Step 3: fan-out – one goroutine per subdirectory, bounded by a semaphore.
	type subdirResult struct {
		path string
		size int64
		err  error
	}

	resultCh := make(chan subdirResult, len(subdirPrefixes))
	sem := make(chan struct{}, opts.Workers)
	var wg sync.WaitGroup

	for _, subPrefix := range subdirPrefixes {
		wg.Add(1)
		sem <- struct{}{}
		go func(sp string) {
			defer wg.Done()
			defer func() { <-sem }()

			size, err := sumPrefix(ctx, client, bucket, sp)
			resultCh <- subdirResult{
				path: fmt.Sprintf("gs://%s/%s", bucket, sp),
				size: size,
				err:  err,
			}
		}(subPrefix)
	}

	wg.Wait()
	close(resultCh)

	// Step 4: collect and aggregate results.
	var duEntries []DUEntry
	total := rootFileTotal
	for r := range resultCh {
		if r.err != nil {
			return nil, r.err
		}
		duEntries = append(duEntries, DUEntry{Path: r.path, Size: r.size})
		total += r.size
	}

	sort.Slice(duEntries, func(i, j int) bool {
		return duEntries[i].Path < duEntries[j].Path
	})

	return &DUResult{
		Entries:  duEntries,
		RootPath: rootPath,
		Total:    total,
	}, nil
}

// DiskUsagePattern finds all GCS paths matching a wildcard pattern and returns
// the total size of each match. Matched subdirectories are summed recursively
// in parallel using the same SetAttrSelection optimisation as DiskUsage.
//
// The pattern should be the raw GCS prefix string (without gs://bucket/), e.g.
// "sdbbn*.infon/" or "logs/2024-*/data*.csv". Wildcards are supported at any
// level of the path hierarchy.
func DiskUsagePattern(ctx context.Context, bucket, pattern string, opts *DUOptions) ([]DUEntry, error) {
	if opts == nil {
		opts = DefaultDUOptions()
	}
	if opts.Workers <= 0 {
		opts.Workers = 8
	}

	client, err := GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	// Strip trailing slash for pattern matching – ListWithPattern works on names
	// without it and will return the directory as a prefix (IsPrefix=true).
	pattern = strings.TrimSuffix(pattern, "/")

	matches, err := ListWithPattern(ctx, bucket, pattern, &ListOptions{
		Recursive: false,
		Delimiter: "/",
	})
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		return nil, nil
	}

	type subdirResult struct {
		path string
		size int64
		err  error
	}

	resultCh := make(chan subdirResult, len(matches))
	sem := make(chan struct{}, opts.Workers)
	var wg sync.WaitGroup

	for _, m := range matches {
		wg.Add(1)
		sem <- struct{}{}
		go func(m *ObjectInfo) {
			defer wg.Done()
			defer func() { <-sem }()

			var size int64
			var sumErr error
			if m.IsPrefix {
				subPrefix := strings.TrimPrefix(m.Path, "gs://"+bucket+"/")
				size, sumErr = sumPrefix(ctx, client, bucket, subPrefix)
			} else {
				size = m.Size
			}
			resultCh <- subdirResult{path: m.Path, size: size, err: sumErr}
		}(m)
	}

	wg.Wait()
	close(resultCh)

	var entries []DUEntry
	for r := range resultCh {
		if r.err != nil {
			return nil, r.err
		}
		entries = append(entries, DUEntry{Path: r.path, Size: r.size})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Path < entries[j].Path
	})

	return entries, nil
}

// sumPrefix returns the total byte size of all objects under a GCS prefix.
// It uses SetAttrSelection to fetch only Name and Size, significantly reducing
// JSON payload and improving throughput for large prefixes.
func sumPrefix(ctx context.Context, client *storage.Client, bucket, prefix string) (int64, error) {
	q := &storage.Query{Prefix: prefix}
	if err := q.SetAttrSelection([]string{"Name", "Size"}); err != nil {
		return 0, fmt.Errorf("SetAttrSelection: %w", err)
	}

	apilog.Logf("[GCS] Objects.List(bucket=%s, prefix=%q) [du sum]", bucket, prefix)
	it := client.Bucket(bucket).Objects(ctx, q)

	var total int64
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("iterating objects under gs://%s/%s: %w", bucket, prefix, err)
		}
		// Skip zero-byte directory placeholder objects (name ends with /).
		if attrs.Size == 0 && strings.HasSuffix(attrs.Name, "/") {
			continue
		}
		total += attrs.Size
	}
	return total, nil
}
