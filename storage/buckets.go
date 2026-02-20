package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/thieso2/cio/apilog"
	"google.golang.org/api/iterator"
)

// BucketInfo holds information about a GCS bucket
type BucketInfo struct {
	Name         string
	Location     string
	StorageClass string
	Created      time.Time
}

// ListBuckets lists all buckets in a GCP project
func ListBuckets(ctx context.Context, projectID string) ([]*BucketInfo, error) {
	client, err := GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	var buckets []*BucketInfo
	apilog.Logf("[GCS] Buckets.List(project=%s)", projectID)
	it := client.Buckets(ctx, projectID)

	for {
		bucketAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate buckets: %w", err)
		}

		buckets = append(buckets, &BucketInfo{
			Name:         bucketAttrs.Name,
			Location:     bucketAttrs.Location,
			StorageClass: bucketAttrs.StorageClass,
			Created:      bucketAttrs.Created,
		})
	}

	return buckets, nil
}

// FormatBucketShort formats bucket info in short format
func FormatBucketShort(bucket *BucketInfo) string {
	return fmt.Sprintf("gs://%s/", bucket.Name)
}

// FormatBucketLong formats bucket info in long format
func FormatBucketLong(bucket *BucketInfo) string {
	timestamp := FormatUnixTime(bucket.Created)
	return fmt.Sprintf("%-15s %-20s %s  gs://%s/",
		timestamp,
		bucket.Location,
		bucket.StorageClass,
		bucket.Name)
}
