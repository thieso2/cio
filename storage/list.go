package storage

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/thieso2/cio/apilog"
	"google.golang.org/api/iterator"
)

// ListOptions configures listing behavior
type ListOptions struct {
	Recursive     bool // List all objects recursively
	LongFormat    bool // Show detailed information
	HumanReadable bool // Show sizes in human-readable format
	Delimiter     string
	MaxResults    int // Maximum number of results (0 = no limit)
}

// DefaultListOptions returns the default listing options
func DefaultListOptions() *ListOptions {
	return &ListOptions{
		Recursive:     false,
		LongFormat:    false,
		HumanReadable: false,
		Delimiter:     "/", // Default delimiter for prefix grouping
		MaxResults:    0,   // No limit by default
	}
}

// List retrieves objects from a GCS bucket with optional prefix
func List(ctx context.Context, bucket, prefix string, opts *ListOptions) ([]*ObjectInfo, error) {
	client, err := GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	if opts == nil {
		opts = DefaultListOptions()
	}

	// Configure query
	query := &storage.Query{
		Prefix: prefix,
	}

	// If not recursive, use delimiter to group by "directories"
	if !opts.Recursive {
		query.Delimiter = opts.Delimiter
	}

	// Execute query
	bucketHandle := client.Bucket(bucket)
	apilog.Logf("[GCS] Objects.List(bucket=%s, prefix=%q, recursive=%v)", bucket, query.Prefix, opts.Recursive)
	it := bucketHandle.Objects(ctx, query)

	var results []*ObjectInfo
	count := 0

	for {
		// Check if we've reached the max results limit
		if opts.MaxResults > 0 && count >= opts.MaxResults {
			break
		}

		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate objects: %w", err)
		}

		// Handle prefixes (directories)
		if attrs.Prefix != "" {
			results = append(results, CreatePrefixInfo(attrs.Prefix, bucket))
			count++
			continue
		}

		// Handle objects
		results = append(results, CreateObjectInfo(attrs, bucket))
		count++
	}

	return results, nil
}

// ListByPath is a convenience function that parses a gs:// path and lists objects
func ListByPath(ctx context.Context, gcsPath string, opts *ListOptions) ([]*ObjectInfo, error) {
	bucket, prefix, err := parseGCSPath(gcsPath)
	if err != nil {
		return nil, err
	}

	return List(ctx, bucket, prefix, opts)
}

// maxParallelRecursiveLists is the maximum number of concurrent recursive GCS
// listing calls issued when expanding a ** wildcard pattern.
const maxParallelRecursiveLists = 8

// ListWithPattern lists objects matching a wildcard pattern using level-by-level
// expansion. The pattern is split into '/' segments and expanded one level at a
// time, so only directories that can possibly match are traversed.
//
// Patterns containing ** are handled in two phases:
//  1. Level-by-level expansion for all segments before the first ** segment.
//  2. Parallel recursive listings from each resulting anchor prefix, filtered
//     by the ** suffix pattern.
//
// Examples:
//
//	"*/dumps/*schema*"      – lists top-level dirs, descends into <x>/dumps/, filters
//	"logs/2024/*.log"       – constant prefix collapsed, single-level filter at the end
//	"2024/*/data.csv"       – lists 2024/ sub-dirs, then checks for exact data.csv
//	"**.csv.zst"            – parallel recursive from root, matches any depth
//	"*/exports/**.csv.zst"  – expands top-level dirs, then parallel recursive per dir
func ListWithPattern(ctx context.Context, bucket, pattern string, opts *ListOptions) ([]*ObjectInfo, error) {
	if opts == nil {
		opts = DefaultListOptions()
	}

	segments := strings.Split(pattern, "/")

	// Find the first segment that contains **.
	doubleStarIdx := -1
	for i, seg := range segments {
		if strings.Contains(seg, "**") {
			doubleStarIdx = i
			break
		}
	}

	// For ** patterns, level-by-level expansion stops at the ** segment.
	// For single-* patterns, it stops before the last segment.
	expandUntil := len(segments) - 1
	if doubleStarIdx != -1 {
		expandUntil = doubleStarIdx
	}

	// Active GCS prefixes we are currently expanding.
	prefixes := []string{""}

	for _, seg := range segments[:expandUntil] {
		if !strings.ContainsAny(seg, "*?") {
			// Constant segment: fold directly into every prefix – no API call.
			for i := range prefixes {
				prefixes[i] += seg + "/"
			}
			continue
		}

		// Wildcard segment: list one level under each prefix, keep dirs that match.
		var next []string
		for _, prefix := range prefixes {
			dirs, err := listDirsMatchingSegment(ctx, bucket, prefix, seg, opts)
			if err != nil {
				return nil, err
			}
			next = append(next, dirs...)
		}
		prefixes = next
		if len(prefixes) == 0 {
			return nil, nil
		}
	}

	// ** path: fan out parallel recursive listings from each anchor prefix.
	if doubleStarIdx != -1 {
		suffixPattern := strings.Join(segments[doubleStarIdx:], "/")
		return listParallelDoubleStarPattern(ctx, bucket, prefixes, suffixPattern, opts)
	}

	// Single-* path: expand the last segment across all active prefixes.
	lastSeg := segments[len(segments)-1]
	var results []*ObjectInfo

	// If the pattern ended with "/" the last segment is empty — the directories
	// found during intermediate expansion ARE the results.
	if lastSeg == "" {
		for _, prefix := range prefixes {
			results = append(results, CreatePrefixInfo(prefix, bucket))
		}
	} else {
		for _, prefix := range prefixes {
			objs, err := listMatchingLastSegment(ctx, bucket, prefix, lastSeg, opts)
			if err != nil {
				return nil, err
			}
			results = append(results, objs...)
		}
	}

	if opts.MaxResults > 0 && len(results) > opts.MaxResults {
		results = results[:opts.MaxResults]
	}
	return results, nil
}

// listParallelDoubleStarPattern issues one recursive GCS listing per anchor
// prefix (letting the GCS API handle the recursion server-side via no delimiter)
// and filters the results client-side against the ** pattern.
//
// Anchors are listed in parallel, capped at maxParallelRecursiveLists goroutines.
// For a single anchor (e.g. **.csv.zst → anchor "") this becomes one API call.
// For multiple anchors (e.g. */exports/**.csv.zst → one anchor per matched dir)
// the parallel goroutines provide real concurrency.
func listParallelDoubleStarPattern(ctx context.Context, bucket string, anchors []string, pattern string, opts *ListOptions) ([]*ObjectInfo, error) {
	type chanResult struct {
		objects []*ObjectInfo
		err     error
	}

	results := make(chan chanResult, len(anchors))
	sem := make(chan struct{}, maxParallelRecursiveLists)

	for _, anchor := range anchors {
		go func(anchor string) {
			sem <- struct{}{}
			defer func() { <-sem }()

			all, err := List(ctx, bucket, anchor, &ListOptions{
				Recursive:     true,
				LongFormat:    opts.LongFormat,
				HumanReadable: opts.HumanReadable,
			})
			if err != nil {
				results <- chanResult{err: err}
				return
			}

			var matched []*ObjectInfo
			for _, obj := range all {
				if obj.IsPrefix {
					continue
				}
				relPath := strings.TrimPrefix(obj.Path, "gs://"+bucket+"/"+anchor)
				if doubleStarMatchPath(relPath, pattern) {
					matched = append(matched, obj)
				}
			}
			results <- chanResult{objects: matched}
		}(anchor)
	}

	var combined []*ObjectInfo
	var firstErr error
	for range len(anchors) {
		r := <-results
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
		combined = append(combined, r.objects...)
	}
	if firstErr != nil {
		return nil, firstErr
	}

	if opts.MaxResults > 0 && len(combined) > opts.MaxResults {
		combined = combined[:opts.MaxResults]
	}
	return combined, nil
}

// listDirsMatchingSegment lists one level below prefix (non-recursive) and
// returns the GCS prefixes of directories whose name matches seg.
func listDirsMatchingSegment(ctx context.Context, bucket, prefix, seg string, opts *ListOptions) ([]string, error) {
	objects, err := List(ctx, bucket, prefix, &ListOptions{
		Recursive: false, Delimiter: "/",
		LongFormat: opts.LongFormat, HumanReadable: opts.HumanReadable,
	})
	if err != nil {
		return nil, err
	}
	var dirs []string
	for _, obj := range objects {
		if !obj.IsPrefix {
			continue
		}
		name := relSegmentName(bucket, prefix, obj)
		if complexWildcardMatch(name, seg) {
			dirs = append(dirs, strings.TrimPrefix(obj.Path, "gs://"+bucket+"/"))
		}
	}
	return dirs, nil
}

// listMatchingLastSegment lists objects at prefix (non-recursive by default,
// recursive when opts.Recursive is set) and returns those whose name matches seg.
func listMatchingLastSegment(ctx context.Context, bucket, prefix, seg string, opts *ListOptions) ([]*ObjectInfo, error) {
	if opts.Recursive {
		// Recursive: flat list under prefix, match the filename portion only.
		all, err := List(ctx, bucket, prefix, &ListOptions{
			Recursive: true,
			LongFormat: opts.LongFormat, HumanReadable: opts.HumanReadable,
		})
		if err != nil {
			return nil, err
		}
		var results []*ObjectInfo
		for _, obj := range all {
			name := relSegmentName(bucket, prefix, obj)
			// For recursive results spanning multiple levels take only the leaf name.
			if idx := strings.LastIndex(name, "/"); idx >= 0 {
				name = name[idx+1:]
			}
			if complexWildcardMatch(name, seg) {
				results = append(results, obj)
			}
		}
		return results, nil
	}

	// Non-recursive: list one level, filter by seg.
	all, err := List(ctx, bucket, prefix, &ListOptions{
		Recursive: false, Delimiter: "/",
		LongFormat: opts.LongFormat, HumanReadable: opts.HumanReadable,
	})
	if err != nil {
		return nil, err
	}
	var results []*ObjectInfo
	for _, obj := range all {
		name := relSegmentName(bucket, prefix, obj)
		if complexWildcardMatch(name, seg) {
			results = append(results, obj)
		}
	}
	return results, nil
}

// relSegmentName returns the single path segment for obj relative to prefix.
// For a directory gs://bucket/a/b/ with prefix "a/" it returns "b".
// For a file gs://bucket/a/b/c.txt with prefix "a/b/" it returns "c.txt".
func relSegmentName(bucket, prefix string, obj *ObjectInfo) string {
	rel := strings.TrimPrefix(obj.Path, "gs://"+bucket+"/")
	name := strings.TrimPrefix(rel, prefix)
	return strings.TrimSuffix(name, "/")
}

// doubleStarMatchPath matches a relative GCS object path against a glob pattern.
//   - ** matches any sequence of characters including /
//   - *  matches any sequence of characters except /
//   - ?  matches any single character except /
func doubleStarMatchPath(text, pattern string) bool {
	if pattern == "" {
		return text == ""
	}
	if len(pattern) >= 2 && pattern[0] == '*' && pattern[1] == '*' {
		p := 2
		for p < len(pattern) && pattern[p] == '*' {
			p++
		}
		// **/ matches zero or more complete path segments
		if p < len(pattern) && pattern[p] == '/' {
			rest := pattern[p+1:]
			if doubleStarMatchPath(text, rest) {
				return true
			}
			for i := 0; i < len(text); i++ {
				if text[i] == '/' {
					if doubleStarMatchPath(text[i+1:], pattern) {
						return true
					}
				}
			}
			return false
		}
		// ** not followed by /: matches zero or more characters including /
		suffix := pattern[p:]
		for i := 0; i <= len(text); i++ {
			if doubleStarMatchPath(text[i:], suffix) {
				return true
			}
		}
		return false
	}
	if pattern[0] == '*' {
		suffix := pattern[1:]
		for i := 0; i <= len(text); i++ {
			if i > 0 && text[i-1] == '/' {
				return false
			}
			if doubleStarMatchPath(text[i:], suffix) {
				return true
			}
		}
		return false
	}
	if pattern[0] == '?' {
		if len(text) == 0 || text[0] == '/' {
			return false
		}
		return doubleStarMatchPath(text[1:], pattern[1:])
	}
	if len(text) == 0 || text[0] != pattern[0] {
		return false
	}
	return doubleStarMatchPath(text[1:], pattern[1:])
}

// parseGCSPath parses a gs:// path into bucket and prefix
func parseGCSPath(gcsPath string) (bucket, prefix string, err error) {
	if !strings.HasPrefix(gcsPath, "gs://") {
		return "", "", fmt.Errorf("invalid GCS path: must start with gs://")
	}

	// Remove gs:// prefix
	pathWithoutPrefix := strings.TrimPrefix(gcsPath, "gs://")

	// Split into bucket and prefix
	parts := strings.SplitN(pathWithoutPrefix, "/", 2)
	bucket = parts[0]

	if bucket == "" {
		return "", "", fmt.Errorf("invalid GCS path: bucket name is required")
	}

	if len(parts) > 1 {
		prefix = parts[1]
	}

	// Check if bucket contains a colon (project-id prefix)
	// If it contains ":" but doesn't end with it, strip the project-id prefix
	// This handles paths like gs://project-id:bucket-name/path
	if strings.Contains(bucket, ":") && !strings.HasSuffix(bucket, ":") {
		colonIdx := strings.Index(bucket, ":")
		bucket = bucket[colonIdx+1:]
	}

	return bucket, prefix, nil
}
