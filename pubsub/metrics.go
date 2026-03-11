package pubsub

import (
	"context"
	"fmt"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/thieso2/cio/apilog"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SubscriptionMetrics holds monitoring metrics for a subscription.
type SubscriptionMetrics struct {
	Undelivered int64
	OldestAge   time.Duration
}

// FetchSubscriptionMetrics fetches num_undelivered_messages and oldest_unacked_message_age
// for one or more subscriptions from Cloud Monitoring.
func FetchSubscriptionMetrics(ctx context.Context, projectID string, subNames []string) (map[string]*SubscriptionMetrics, error) {
	client, err := GetMonitoringClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create monitoring client: %w", err)
	}

	result := make(map[string]*SubscriptionMetrics)
	for _, name := range subNames {
		result[name] = &SubscriptionMetrics{}
	}

	now := time.Now()
	start := now.Add(-10 * time.Minute)

	// Fetch undelivered messages
	fetchMetric(ctx, client, projectID, "pubsub.googleapis.com/subscription/num_undelivered_messages", subNames, start, now, func(subID string, value float64) {
		if m, ok := result[subID]; ok {
			m.Undelivered = int64(value)
		}
	})

	// Fetch oldest unacked message age
	fetchMetric(ctx, client, projectID, "pubsub.googleapis.com/subscription/oldest_unacked_message_age", subNames, start, now, func(subID string, value float64) {
		if m, ok := result[subID]; ok {
			m.OldestAge = time.Duration(value) * time.Second
		}
	})

	return result, nil
}

func fetchMetric(ctx context.Context, client *monitoring.MetricClient, projectID, metricType string, subNames []string, start, end time.Time, apply func(string, float64)) {
	// Build filter for the metric
	filter := fmt.Sprintf(`metric.type = "%s" AND resource.type = "pubsub_subscription"`, metricType)

	apilog.Logf("[PubSub] monitoring.ListTimeSeries(project=%s, metric=%s)", projectID, metricType)

	it := client.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:   fmt.Sprintf("projects/%s", projectID),
		Filter: filter,
		Interval: &monitoringpb.TimeInterval{
			StartTime: timestamppb.New(start),
			EndTime:   timestamppb.New(end),
		},
		Aggregation: &monitoringpb.Aggregation{
			AlignmentPeriod:    durationpb.New(5 * time.Minute),
			PerSeriesAligner:   monitoringpb.Aggregation_ALIGN_MEAN,
		},
	})

	// Build a set of subscription names we care about
	wanted := make(map[string]bool)
	for _, n := range subNames {
		wanted[n] = true
	}

	for {
		ts, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			apilog.Logf("[PubSub] monitoring error: %v", err)
			return
		}

		// Extract subscription_id from resource labels
		subID := ts.GetResource().GetLabels()["subscription_id"]
		if !wanted[subID] {
			continue
		}

		// Get most recent data point
		points := ts.GetPoints()
		if len(points) == 0 {
			continue
		}

		// Points are ordered newest first
		point := points[0]
		value := point.GetValue().GetInt64Value()
		if value == 0 {
			value = int64(point.GetValue().GetDoubleValue())
		}
		apply(subID, float64(value))
	}
}

// StreamMetrics prints subscription metrics at regular intervals.
// If follow is false, prints a single snapshot.
func StreamMetrics(ctx context.Context, projectID, subName string, follow bool, interval time.Duration) error {
	printSnapshot := func() error {
		metrics, err := FetchSubscriptionMetrics(ctx, projectID, []string{subName})
		if err != nil {
			return err
		}
		m := metrics[subName]
		ts := time.Now().Format("2006-01-02 15:04:05")
		oldest := "0s"
		if m.OldestAge > 0 {
			oldest = formatDuration(m.OldestAge)
		}
		fmt.Printf("%s  undelivered: %s  oldest: %s\n", ts, formatCount(m.Undelivered), oldest)
		return nil
	}

	if err := printSnapshot(); err != nil {
		return err
	}
	if !follow {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
			if err := printSnapshot(); err != nil {
				apilog.Logf("[PubSub] metric poll error: %v", err)
			}
		}
	}
}
