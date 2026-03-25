package pubsub

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/thieso2/cio/apilog"
	"google.golang.org/api/iterator"
)

// TopicInfo holds information about a Pub/Sub topic.
type TopicInfo struct {
	Name              string            `json:"name"`
	Project           string            `json:"project"`
	SubscriptionCount int               `json:"subscription_count"`
	Retention         time.Duration     `json:"retention,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	KMSKeyName        string            `json:"kms_key_name,omitempty"`
	SchemaSettings    *pubsub.SchemaSettings `json:"schema_settings,omitempty"`
}

// FormatShort returns a short one-line representation.
func (t *TopicInfo) FormatShort() string {
	return t.Name
}

// FormatLong returns a long one-line representation.
func (t *TopicInfo) FormatLong() string {
	retention := "-"
	if t.Retention > 0 {
		retention = formatDuration(t.Retention)
	}
	labels := formatLabels(t.Labels)
	return fmt.Sprintf("%-50s %4d  %10s  %s", t.Name, t.SubscriptionCount, retention, labels)
}

// TopicLongHeader returns the header for long topic listing.
func TopicLongHeader() string {
	return fmt.Sprintf("%-50s %4s  %10s  %s", "NAME", "SUBS", "RETENTION", "LABELS")
}

// SubscriptionInfo holds information about a Pub/Sub subscription.
type SubscriptionInfo struct {
	Name                string            `json:"name"`
	Project             string            `json:"project"`
	TopicName           string            `json:"topic_name"`
	Type                string            `json:"type"`
	AckDeadline         time.Duration     `json:"ack_deadline"`
	Filter              string            `json:"filter,omitempty"`
	PushEndpoint        string            `json:"push_endpoint,omitempty"`
	DeadLetterTopic     string            `json:"dead_letter_topic,omitempty"`
	MaxDeliveryAttempts int               `json:"max_delivery_attempts,omitempty"`
	Retention           time.Duration     `json:"retention,omitempty"`
	Labels              map[string]string `json:"labels,omitempty"`

	// Metrics (populated separately via Cloud Monitoring)
	Undelivered int64         `json:"undelivered,omitempty"`
	OldestAge   time.Duration `json:"oldest_age,omitempty"`
	HasMetrics  bool          `json:"-"`
}

// FormatShort returns a short one-line representation.
func (s *SubscriptionInfo) FormatShort() string {
	return s.Name
}

// FormatLong returns a long one-line representation.
func (s *SubscriptionInfo) FormatLong() string {
	topicShort := shortTopicName(s.TopicName)
	ackDeadline := fmt.Sprintf("%ds", int(s.AckDeadline.Seconds()))

	undelivered := "-"
	oldest := "-"
	if s.HasMetrics {
		undelivered = formatCount(s.Undelivered)
		if s.OldestAge > 0 {
			oldest = formatDuration(s.OldestAge)
		} else {
			oldest = "0s"
		}
	}

	return fmt.Sprintf("%-50s %-40s %-6s %5s  %10s  %8s", s.Name, topicShort, s.Type, ackDeadline, undelivered, oldest)
}

// SubscriptionLongHeader returns the header for long subscription listing.
func SubscriptionLongHeader() string {
	return fmt.Sprintf("%-50s %-40s %-6s %5s  %10s  %8s", "NAME", "TOPIC", "TYPE", "ACK", "UNDELIVERED", "OLDEST")
}

// ListTopics lists all topics in the project.
// If longFormat is true, fetches config and subscription counts (slower).
func ListTopics(ctx context.Context, projectID string, longFormat bool) ([]*TopicInfo, error) {
	client, err := GetClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	apilog.Logf("[PubSub] Topics.List(project=%s)", projectID)
	it := client.Topics(ctx)

	var topics []*TopicInfo
	for {
		topic, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list topics: %w", err)
		}

		info := &TopicInfo{
			Name:    topic.ID(),
			Project: projectID,
		}

		if longFormat {
			cfg, err := topic.Config(ctx)
			if err != nil {
				apilog.Logf("[PubSub] failed to get topic config for %s: %v", topic.ID(), err)
			} else {
				info.Retention = optionalDuration(cfg.RetentionDuration)
				info.Labels = cfg.Labels
				info.KMSKeyName = cfg.KMSKeyName
				info.SchemaSettings = cfg.SchemaSettings
			}

			subCount := 0
			subIt := topic.Subscriptions(ctx)
			for {
				_, err := subIt.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					break
				}
				subCount++
			}
			info.SubscriptionCount = subCount
		}

		topics = append(topics, info)
	}
	return topics, nil
}

// ListSubscriptions lists all subscriptions in the project.
// If longFormat is true, fetches config details (slower).
func ListSubscriptions(ctx context.Context, projectID string, longFormat bool) ([]*SubscriptionInfo, error) {
	client, err := GetClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	apilog.Logf("[PubSub] Subscriptions.List(project=%s)", projectID)
	it := client.Subscriptions(ctx)

	var subs []*SubscriptionInfo
	for {
		sub, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list subscriptions: %w", err)
		}

		if longFormat {
			cfg, err := sub.Config(ctx)
			if err != nil {
				apilog.Logf("[PubSub] failed to get subscription config for %s: %v", sub.ID(), err)
				subs = append(subs, &SubscriptionInfo{Name: sub.ID(), Project: projectID})
				continue
			}
			subs = append(subs, subscriptionConfigToInfo(sub.ID(), projectID, cfg))
		} else {
			subs = append(subs, &SubscriptionInfo{Name: sub.ID(), Project: projectID})
		}
	}
	return subs, nil
}

// GetTopic gets detailed info about a specific topic.
func GetTopic(ctx context.Context, projectID, topicName string) (*TopicInfo, error) {
	client, err := GetClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	apilog.Logf("[PubSub] Topics.Get(project=%s, topic=%s)", projectID, topicName)
	topic := client.Topic(topicName)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check topic %s: %w", topicName, err)
	}
	if !exists {
		return nil, fmt.Errorf("topic %q not found", topicName)
	}

	cfg, err := topic.Config(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic config: %w", err)
	}

	subCount := 0
	subIt := topic.Subscriptions(ctx)
	for {
		_, err := subIt.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			break
		}
		subCount++
	}

	return &TopicInfo{
		Name:              topic.ID(),
		Project:           projectID,
		SubscriptionCount: subCount,
		Retention:         optionalDuration(cfg.RetentionDuration),
		Labels:            cfg.Labels,
		KMSKeyName:        cfg.KMSKeyName,
		SchemaSettings:    cfg.SchemaSettings,
	}, nil
}

// GetSubscription gets detailed info about a specific subscription.
func GetSubscription(ctx context.Context, projectID, subName string) (*SubscriptionInfo, error) {
	client, err := GetClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	apilog.Logf("[PubSub] Subscriptions.Get(project=%s, sub=%s)", projectID, subName)
	sub := client.Subscription(subName)
	exists, err := sub.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check subscription %s: %w", subName, err)
	}
	if !exists {
		return nil, fmt.Errorf("subscription %q not found", subName)
	}

	cfg, err := sub.Config(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get subscription config: %w", err)
	}

	return subscriptionConfigToInfo(sub.ID(), projectID, cfg), nil
}

// ListTopicSubscriptions lists subscriptions attached to a specific topic.
func ListTopicSubscriptions(ctx context.Context, projectID, topicName string) ([]*SubscriptionInfo, error) {
	client, err := GetClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	apilog.Logf("[PubSub] Topics.Subscriptions(project=%s, topic=%s)", projectID, topicName)
	topic := client.Topic(topicName)

	var subs []*SubscriptionInfo
	it := topic.Subscriptions(ctx)
	for {
		sub, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list topic subscriptions: %w", err)
		}

		cfg, err := sub.Config(ctx)
		if err != nil {
			continue // non-fatal
		}

		subs = append(subs, subscriptionConfigToInfo(sub.ID(), projectID, cfg))
	}
	return subs, nil
}

// DeleteTopic deletes a topic.
func DeleteTopic(ctx context.Context, projectID, topicName string) error {
	client, err := GetClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	apilog.Logf("[PubSub] Topics.Delete(project=%s, topic=%s)", projectID, topicName)
	topic := client.Topic(topicName)
	return topic.Delete(ctx)
}

// DeleteSubscription deletes a subscription.
func DeleteSubscription(ctx context.Context, projectID, subName string) error {
	client, err := GetClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	apilog.Logf("[PubSub] Subscriptions.Delete(project=%s, sub=%s)", projectID, subName)
	sub := client.Subscription(subName)
	return sub.Delete(ctx)
}

func subscriptionConfigToInfo(id, projectID string, cfg pubsub.SubscriptionConfig) *SubscriptionInfo {
	subType := "pull"
	pushEndpoint := ""
	if cfg.PushConfig.Endpoint != "" {
		subType = "push"
		pushEndpoint = cfg.PushConfig.Endpoint
	}

	topicName := ""
	if cfg.Topic != nil {
		topicName = cfg.Topic.ID()
	}

	dlTopic := ""
	maxAttempts := 0
	if cfg.DeadLetterPolicy != nil {
		dlTopic = shortTopicName(cfg.DeadLetterPolicy.DeadLetterTopic)
		maxAttempts = cfg.DeadLetterPolicy.MaxDeliveryAttempts
	}

	return &SubscriptionInfo{
		Name:                id,
		Project:             projectID,
		TopicName:           topicName,
		Type:                subType,
		AckDeadline:         cfg.AckDeadline,
		Filter:              cfg.Filter,
		PushEndpoint:        pushEndpoint,
		DeadLetterTopic:     dlTopic,
		MaxDeliveryAttempts: maxAttempts,
		Retention:           cfg.RetentionDuration,
		Labels:              cfg.Labels,
	}
}

// shortTopicName extracts the topic name from a full resource path.
// e.g. "projects/my-project/topics/my-topic" → "my-topic"
func shortTopicName(fullPath string) string {
	if idx := strings.LastIndex(fullPath, "/"); idx != -1 {
		return fullPath[idx+1:]
	}
	return fullPath
}

func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return "-"
	}
	var parts []string
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ", ")
}

func formatDuration(d time.Duration) string {
	if d >= 24*time.Hour {
		days := int(d.Hours() / 24)
		return fmt.Sprintf("%dd", days)
	}
	if d >= time.Hour {
		return fmt.Sprintf("%.0fh", d.Hours())
	}
	if d >= time.Minute {
		return fmt.Sprintf("%.0fm", d.Minutes())
	}
	return fmt.Sprintf("%.0fs", d.Seconds())
}

func formatCount(n int64) string {
	if n >= 1_000_000 {
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%.1fK", float64(n)/1_000)
	}
	return fmt.Sprintf("%d", n)
}

// optionalDuration extracts a time.Duration from an optional.Duration interface value.
func optionalDuration(v interface{}) time.Duration {
	if v == nil {
		return 0
	}
	if d, ok := v.(time.Duration); ok {
		return d
	}
	return 0
}

// ParsePubSubPath parses a pubsub:// path into components.
// Returns (resourceType, name) where resourceType is "topics", "subs", or "".
//
//	pubsub://              → ("", "")         list all
//	pubsub://topics        → ("topics", "")   list topics
//	pubsub://subs          → ("subs", "")     list subs
//	pubsub://topics/name   → ("topics", "name")
//	pubsub://subs/name     → ("subs", "name")
func ParsePubSubPath(p string) (resourceType, name string) {
	rest := strings.TrimPrefix(p, "pubsub://")
	rest = strings.TrimRight(rest, "/")
	if rest == "" {
		return "", ""
	}
	parts := strings.SplitN(rest, "/", 2)
	resourceType = parts[0]
	if len(parts) > 1 {
		name = parts[1]
	}
	return
}
