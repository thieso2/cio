package resource

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/thieso2/cio/pubsub"
)

const TypePubSub Type = "pubsub"

// PubSubResource implements Resource for Pub/Sub topics and subscriptions.
type PubSubResource struct {
	formatter   PathFormatter
	listingMode string // "topics", "subs", or "both" — set by List()
}

// CreatePubSubResource creates a new Pub/Sub resource handler.
func CreatePubSubResource(formatter PathFormatter) *PubSubResource {
	return &PubSubResource{formatter: formatter}
}

func (r *PubSubResource) Type() Type         { return TypePubSub }
func (r *PubSubResource) SupportsInfo() bool { return true }

func (r *PubSubResource) ParsePath(p string) (*PathComponents, error) {
	return &PathComponents{ResourceType: TypePubSub}, nil
}

func (r *PubSubResource) FormatLongHeader() string {
	switch r.listingMode {
	case "topics":
		return pubsub.TopicLongHeader()
	case "subs":
		return pubsub.SubscriptionLongHeader()
	default:
		return ""
	}
}

// List lists Pub/Sub topics and/or subscriptions.
func (r *PubSubResource) List(ctx context.Context, p string, opts *ListOptions) ([]*ResourceInfo, error) {
	var project string
	if opts != nil {
		project = opts.ProjectID
	}
	if project == "" {
		return nil, fmt.Errorf("project ID is required for Pub/Sub (use --project flag or set defaults.project_id in config)")
	}

	resType, namePattern := pubsub.ParsePubSubPath(p)

	switch resType {
	case "", "topics":
		if resType == "" {
			// List both topics and subs
			return r.listBoth(ctx, project, namePattern, opts)
		}
		return r.listTopics(ctx, project, namePattern, opts)
	case "subs":
		return r.listSubs(ctx, project, namePattern, opts)
	default:
		return nil, fmt.Errorf("unknown pubsub resource type: %s (use pubsub://topics or pubsub://subs)", resType)
	}
}

func (r *PubSubResource) listBoth(ctx context.Context, project, namePattern string, opts *ListOptions) ([]*ResourceInfo, error) {
	r.listingMode = "both"

	topics, err := r.listTopics(ctx, project, namePattern, opts)
	if err != nil {
		return nil, err
	}
	subs, err := r.listSubs(ctx, project, namePattern, opts)
	if err != nil {
		return nil, err
	}

	r.listingMode = "both"
	return append(topics, subs...), nil
}

func (r *PubSubResource) listTopics(ctx context.Context, project, namePattern string, opts *ListOptions) ([]*ResourceInfo, error) {
	r.listingMode = "topics"

	longFormat := opts != nil && opts.LongFormat
	topics, err := pubsub.ListTopics(ctx, project, longFormat)
	if err != nil {
		return nil, err
	}

	var resources []*ResourceInfo
	for _, t := range topics {
		if namePattern != "" {
			if ok, _ := path.Match(namePattern, t.Name); !ok {
				continue
			}
		}
		resources = append(resources, &ResourceInfo{
			Name:     t.Name,
			Path:     "pubsub://topics/" + t.Name,
			Type:     "topic",
			Metadata: t,
		})
	}
	return resources, nil
}

func (r *PubSubResource) listSubs(ctx context.Context, project, namePattern string, opts *ListOptions) ([]*ResourceInfo, error) {
	r.listingMode = "subs"

	longFormat := opts != nil && opts.LongFormat
	subs, err := pubsub.ListSubscriptions(ctx, project, longFormat)
	if err != nil {
		return nil, err
	}

	// Fetch metrics for long format
	if opts != nil && opts.LongFormat {
		var subNames []string
		for _, s := range subs {
			subNames = append(subNames, s.Name)
		}
		if len(subNames) > 0 {
			metrics, err := pubsub.FetchSubscriptionMetrics(ctx, project, subNames)
			if err == nil {
				for _, s := range subs {
					if m, ok := metrics[s.Name]; ok {
						s.Undelivered = m.Undelivered
						s.OldestAge = m.OldestAge
						s.HasMetrics = true
					}
				}
			}
			// Degrade gracefully if metrics fail
		}
	}

	var resources []*ResourceInfo
	for _, s := range subs {
		if namePattern != "" {
			if ok, _ := path.Match(namePattern, s.Name); !ok {
				continue
			}
		}
		resources = append(resources, &ResourceInfo{
			Name:     s.Name,
			Path:     "pubsub://subs/" + s.Name,
			Type:     "subscription",
			Metadata: s,
		})
	}
	return resources, nil
}

// Info returns detailed information about a topic or subscription.
func (r *PubSubResource) Info(_ context.Context, p string) (*ResourceInfo, error) {
	_, name := pubsub.ParsePubSubPath(p)
	return nil, fmt.Errorf("use InfoWithProject for pubsub info (resource: %s)", name)
}

// InfoWithProject returns detailed information with explicit project ID.
func (r *PubSubResource) InfoWithProject(ctx context.Context, p string, projectID string) (*ResourceInfo, error) {
	resType, name := pubsub.ParsePubSubPath(p)
	if name == "" {
		return nil, fmt.Errorf("resource name is required for info (e.g., pubsub://topics/my-topic)")
	}

	switch resType {
	case "topics":
		return r.topicInfo(ctx, projectID, name)
	case "subs":
		return r.subInfo(ctx, projectID, name)
	default:
		return nil, fmt.Errorf("info requires pubsub://topics/NAME or pubsub://subs/NAME")
	}
}

func (r *PubSubResource) topicInfo(ctx context.Context, projectID, name string) (*ResourceInfo, error) {
	topic, err := pubsub.GetTopic(ctx, projectID, name)
	if err != nil {
		return nil, err
	}

	// Get attached subscriptions
	topicSubs, err := pubsub.ListTopicSubscriptions(ctx, projectID, name)
	if err != nil {
		topicSubs = nil // non-fatal
	}

	// Find dead letter references: scan all subscriptions for ones using this topic as dead letter target
	allSubs, _ := pubsub.ListSubscriptions(ctx, projectID, false)
	var dlRefs []*pubsub.SubscriptionInfo
	for _, s := range allSubs {
		if s.DeadLetterTopic == name {
			dlRefs = append(dlRefs, s)
		}
	}

	details := &topicDetails{
		Topic:           topic,
		Subscriptions:   topicSubs,
		DeadLetterRefs:  dlRefs,
	}

	return &ResourceInfo{
		Name:     name,
		Path:     "pubsub://topics/" + name,
		Type:     "topic",
		Details:  details,
		Metadata: topic,
	}, nil
}

func (r *PubSubResource) subInfo(ctx context.Context, projectID, name string) (*ResourceInfo, error) {
	sub, err := pubsub.GetSubscription(ctx, projectID, name)
	if err != nil {
		return nil, err
	}

	// Fetch metrics
	metrics, err := pubsub.FetchSubscriptionMetrics(ctx, projectID, []string{name})
	if err == nil {
		if m, ok := metrics[name]; ok {
			sub.Undelivered = m.Undelivered
			sub.OldestAge = m.OldestAge
			sub.HasMetrics = true
		}
	}

	// Find sibling subscriptions on the same topic
	var siblings []*pubsub.SubscriptionInfo
	if sub.TopicName != "" {
		topicSubs, err := pubsub.ListTopicSubscriptions(ctx, projectID, sub.TopicName)
		if err == nil {
			for _, s := range topicSubs {
				if s.Name != name {
					siblings = append(siblings, s)
				}
			}
		}
	}

	details := &subDetails{
		Subscription: sub,
		Siblings:     siblings,
	}

	return &ResourceInfo{
		Name:     name,
		Path:     "pubsub://subs/" + name,
		Type:     "subscription",
		Details:  details,
		Metadata: sub,
	}, nil
}

type topicDetails struct {
	Topic          *pubsub.TopicInfo
	Subscriptions  []*pubsub.SubscriptionInfo
	DeadLetterRefs []*pubsub.SubscriptionInfo
}

type subDetails struct {
	Subscription *pubsub.SubscriptionInfo
	Siblings     []*pubsub.SubscriptionInfo
}

// Remove deletes a topic or subscription.
func (r *PubSubResource) Remove(ctx context.Context, p string, opts *RemoveOptions) error {
	resType, name := pubsub.ParsePubSubPath(p)
	if name == "" {
		return fmt.Errorf("resource name is required for removal")
	}

	var project string
	if opts != nil {
		project = opts.Project
	}
	if project == "" {
		return fmt.Errorf("project ID is required for Pub/Sub removal")
	}

	// Handle wildcards
	if strings.ContainsAny(name, "*?") {
		return r.removeWithWildcard(ctx, project, resType, name, opts)
	}

	switch resType {
	case "topics":
		return r.removeTopic(ctx, project, name, opts)
	case "subs":
		return r.removeSub(ctx, project, name, opts)
	default:
		return fmt.Errorf("remove requires pubsub://topics/NAME or pubsub://subs/NAME")
	}
}

func (r *PubSubResource) removeTopic(ctx context.Context, project, name string, opts *RemoveOptions) error {
	// Check for orphaned subscriptions
	topicSubs, err := pubsub.ListTopicSubscriptions(ctx, project, name)
	if err == nil && len(topicSubs) > 0 {
		fmt.Printf("Warning: topic %q has %d subscription(s) that will be orphaned:\n", name, len(topicSubs))
		for _, s := range topicSubs {
			fmt.Printf("  - %s\n", s.Name)
		}
		fmt.Println()
	}

	if opts == nil || !opts.Force {
		fmt.Printf("Delete topic %s? (y/N): ", name)
		var response string
		fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			fmt.Println("Cancelled.")
			return nil
		}
	}

	if err := pubsub.DeleteTopic(ctx, project, name); err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}
	fmt.Printf("Deleted topic: %s\n", name)
	return nil
}

func (r *PubSubResource) removeSub(ctx context.Context, project, name string, opts *RemoveOptions) error {
	if opts == nil || !opts.Force {
		fmt.Printf("Delete subscription %s? (y/N): ", name)
		var response string
		fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			fmt.Println("Cancelled.")
			return nil
		}
	}

	if err := pubsub.DeleteSubscription(ctx, project, name); err != nil {
		return fmt.Errorf("failed to delete subscription: %w", err)
	}
	fmt.Printf("Deleted subscription: %s\n", name)
	return nil
}

func (r *PubSubResource) removeWithWildcard(ctx context.Context, project, resType, pattern string, opts *RemoveOptions) error {
	// List matching resources
	var items []string
	switch resType {
	case "topics":
		topics, err := pubsub.ListTopics(ctx, project, false)
		if err != nil {
			return err
		}
		for _, t := range topics {
			if ok, _ := path.Match(pattern, t.Name); ok {
				items = append(items, t.Name)
			}
		}
	case "subs":
		subs, err := pubsub.ListSubscriptions(ctx, project, false)
		if err != nil {
			return err
		}
		for _, s := range subs {
			if ok, _ := path.Match(pattern, s.Name); ok {
				items = append(items, s.Name)
			}
		}
	default:
		return fmt.Errorf("wildcard remove requires pubsub://topics/PATTERN or pubsub://subs/PATTERN")
	}

	if len(items) == 0 {
		fmt.Println("No matching resources found.")
		return nil
	}

	resourceWord := resType
	fmt.Printf("Found %d matching %s:\n", len(items), resourceWord)
	for _, name := range items {
		fmt.Printf("  - %s\n", name)
	}
	fmt.Println()

	if opts == nil || !opts.Force {
		fmt.Printf("Delete all %d %s? (y/N): ", len(items), resourceWord)
		var response string
		fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			fmt.Println("Cancelled.")
			return nil
		}
	}

	for _, name := range items {
		var err error
		switch resType {
		case "topics":
			err = pubsub.DeleteTopic(ctx, project, name)
		case "subs":
			err = pubsub.DeleteSubscription(ctx, project, name)
		}
		if err != nil {
			fmt.Printf("  error deleting %s: %v\n", name, err)
		} else {
			fmt.Printf("  deleted %s\n", name)
		}
	}
	return nil
}

func (r *PubSubResource) FormatShort(info *ResourceInfo, _ string) string {
	switch m := info.Metadata.(type) {
	case *pubsub.TopicInfo:
		return m.FormatShort()
	case *pubsub.SubscriptionInfo:
		return m.FormatShort()
	}
	return info.Name
}

func (r *PubSubResource) FormatLong(info *ResourceInfo, _ string) string {
	switch m := info.Metadata.(type) {
	case *pubsub.TopicInfo:
		return m.FormatLong()
	case *pubsub.SubscriptionInfo:
		return m.FormatLong()
	}
	return info.Name
}

func (r *PubSubResource) FormatDetailed(info *ResourceInfo, aliasPath string) string {
	var b strings.Builder

	switch d := info.Details.(type) {
	case *topicDetails:
		t := d.Topic
		fmt.Fprintf(&b, "Topic: %s\n", t.Name)
		fmt.Fprintf(&b, "  Project:       %s\n", t.Project)
		if t.Retention > 0 {
			fmt.Fprintf(&b, "  Retention:     %s\n", formatDurationLong(t.Retention))
		}
		if t.KMSKeyName != "" {
			fmt.Fprintf(&b, "  KMS Key:       %s\n", t.KMSKeyName)
		}
		if t.SchemaSettings != nil {
			fmt.Fprintf(&b, "  Schema:        %s (encoding: %s)\n", t.SchemaSettings.Schema, schemaEncodingName(int32(t.SchemaSettings.Encoding)))
		}
		if len(t.Labels) > 0 {
			fmt.Fprintf(&b, "  Labels:        %s\n", formatLabelsLong(t.Labels))
		}

		fmt.Fprintf(&b, "\nSubscriptions (%d):\n", len(d.Subscriptions))
		if len(d.Subscriptions) == 0 {
			fmt.Fprintf(&b, "  (none)\n")
		}
		for _, s := range d.Subscriptions {
			fmt.Fprintf(&b, "  - %s (%s", s.Name, s.Type)
			if s.Filter != "" {
				fmt.Fprintf(&b, ", filter: %s", s.Filter)
			}
			if s.DeadLetterTopic != "" {
				fmt.Fprintf(&b, ", dead-letter: %s", s.DeadLetterTopic)
			}
			if s.PushEndpoint != "" {
				fmt.Fprintf(&b, ", push: %s", s.PushEndpoint)
			}
			fmt.Fprintf(&b, ")\n")
		}

		if len(d.DeadLetterRefs) > 0 {
			fmt.Fprintf(&b, "\nDead Letter References:\n")
			for _, s := range d.DeadLetterRefs {
				fmt.Fprintf(&b, "  - %s (source topic: %s)\n", s.Name, s.TopicName)
			}
		}

	case *subDetails:
		s := d.Subscription
		fmt.Fprintf(&b, "Subscription: %s\n", s.Name)
		fmt.Fprintf(&b, "  Project:       %s\n", s.Project)
		fmt.Fprintf(&b, "  Topic:         %s\n", s.TopicName)
		fmt.Fprintf(&b, "  Type:          %s\n", s.Type)
		fmt.Fprintf(&b, "  Ack Deadline:  %s\n", formatDurationLong(s.AckDeadline))
		if s.Filter != "" {
			fmt.Fprintf(&b, "  Filter:        %s\n", s.Filter)
		}
		if s.PushEndpoint != "" {
			fmt.Fprintf(&b, "  Push Endpoint: %s\n", s.PushEndpoint)
		}
		if s.DeadLetterTopic != "" {
			fmt.Fprintf(&b, "  Dead Letter:   %s (max attempts: %d)\n", s.DeadLetterTopic, s.MaxDeliveryAttempts)
		}
		if s.Retention > 0 {
			fmt.Fprintf(&b, "  Retention:     %s\n", formatDurationLong(s.Retention))
		}
		if len(s.Labels) > 0 {
			fmt.Fprintf(&b, "  Labels:        %s\n", formatLabelsLong(s.Labels))
		}

		if s.HasMetrics {
			fmt.Fprintf(&b, "\nMetrics:\n")
			fmt.Fprintf(&b, "  Undelivered:   %s\n", formatCountLong(s.Undelivered))
			oldest := "0s"
			if s.OldestAge > 0 {
				oldest = formatDurationLong(s.OldestAge)
			}
			fmt.Fprintf(&b, "  Oldest Msg:    %s\n", oldest)
		}

		if len(d.Siblings) > 0 {
			fmt.Fprintf(&b, "\nSibling Subscriptions (same topic):\n")
			for _, sib := range d.Siblings {
				fmt.Fprintf(&b, "  - %s (%s)\n", sib.Name, sib.Type)
			}
		}

	default:
		return r.FormatLong(info, aliasPath)
	}

	return b.String()
}

func schemaEncodingName(e int32) string {
	switch e {
	case 1:
		return "JSON"
	case 2:
		return "BINARY"
	default:
		return "UNSPECIFIED"
	}
}

func formatDurationLong(d time.Duration) string {
	if d >= 24*time.Hour {
		days := int(d.Hours() / 24)
		hours := int(d.Hours()) % 24
		if hours > 0 {
			return fmt.Sprintf("%dd%dh", days, hours)
		}
		return fmt.Sprintf("%dd", days)
	}
	if d >= time.Hour {
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	}
	if d >= time.Minute {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%ds", int(d.Seconds()))
}

func formatLabelsLong(labels map[string]string) string {
	var parts []string
	for k, v := range labels {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ", ")
}

func formatCountLong(n int64) string {
	if n >= 1_000_000 {
		return fmt.Sprintf("%d (%.1fM)", n, float64(n)/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%d (%.1fK)", n, float64(n)/1_000)
	}
	return fmt.Sprintf("%d", n)
}
