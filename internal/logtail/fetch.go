package logtail

import (
	"context"
	"fmt"
	"sort"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/logadmin"
	"google.golang.org/api/iterator"
)

// Tagged pairs a fetched entry with the index of the filter (within the slice
// passed to Fetch) that produced it. Single-filter callers ignore Filter;
// multi-filter callers (e.g. Dataflow's [J]/[W]/[S] split) use it to recover
// which filter an entry matched.
type Tagged struct {
	Entry  *logging.Entry
	Filter int
}

// Fetch returns up to n of the most recent entries per filter, merged into a
// single slice in chronological order (oldest first). A single logadmin client
// is shared across all filters. n is applied per filter, matching the prior
// per-job / per-log-type behaviour.
func Fetch(ctx context.Context, projectID string, filters []string, n int) ([]Tagged, error) {
	client, err := logadmin.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("creating logging client: %w", err)
	}
	defer client.Close()

	var all []Tagged
	for i, filter := range filters {
		it := client.Entries(ctx, logadmin.Filter(filter), logadmin.NewestFirst())
		count := 0
		for count < n {
			entry, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("fetching log entries: %w", err)
			}
			all = append(all, Tagged{Entry: entry, Filter: i})
			count++
		}
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Entry.Timestamp.Before(all[j].Entry.Timestamp)
	})
	return all, nil
}
