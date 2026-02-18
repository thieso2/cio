package storage

import (
	"context"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// CatObject streams a single GCS object to w.
func CatObject(ctx context.Context, client *storage.Client, bucket, object string, w io.Writer) error {
	reader, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("failed to open gs://%s/%s: %w", bucket, object, err)
	}
	defer reader.Close()

	if _, err := io.Copy(w, reader); err != nil {
		return fmt.Errorf("failed to read gs://%s/%s: %w", bucket, object, err)
	}
	return nil
}

// CatWithPattern streams all GCS objects matching a wildcard pattern to w.
// Objects are streamed in the order they are returned by the API.
func CatWithPattern(ctx context.Context, client *storage.Client, bucket, pattern string, w io.Writer) error {
	prefix, wildcardPattern := splitPattern(pattern)

	bkt := client.Bucket(bucket)
	query := &storage.Query{Prefix: prefix}

	it := bkt.Objects(ctx, query)
	found := 0
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
		if !matchesPattern(attrs.Name, wildcardPattern) {
			continue
		}
		found++
		if err := CatObject(ctx, client, bucket, attrs.Name, w); err != nil {
			return err
		}
	}

	if found == 0 {
		return fmt.Errorf("no objects found matching pattern: gs://%s/%s", bucket, pattern)
	}
	return nil
}
