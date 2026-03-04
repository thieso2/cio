package cloudrun

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/logadmin"
	logpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/fatih/color"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/protobuf/types/known/durationpb"
)

// LogFilter builds a Cloud Logging filter for a Cloud Run resource.
//
// For jobs, the execution argument controls scope:
//
//	""         – job-level logs only (no execution label, no audit logs)
//	"*"        – all execution logs (execution label present, any value)
//	"<id>"     – logs for one specific execution
func LogFilter(projectID, region, scheme, name, execution string) string {
	var parts []string

	switch scheme {
	case "svc":
		parts = append(parts, `resource.type="cloud_run_revision"`)
		if name != "" {
			parts = append(parts, fmt.Sprintf(`resource.labels.service_name="%s"`, name))
		}
	case "jobs":
		parts = append(parts, `resource.type="cloud_run_job"`)
		if name != "" {
			parts = append(parts, fmt.Sprintf(`resource.labels.job_name="%s"`, name))
		}
		if region != "" {
			parts = append(parts, fmt.Sprintf(`resource.labels.location="%s"`, region))
		}
		switch execution {
		case "":
			// Job-level system logs: no execution label, exclude audit noise
			parts = append(parts, `NOT labels."run.googleapis.com/execution_name":*`)
			parts = append(parts, `NOT log_name:"cloudaudit"`)
		case "*":
			// All executions: require execution label to be present
			parts = append(parts, `labels."run.googleapis.com/execution_name":*`)
		default:
			// Specific execution
			parts = append(parts, fmt.Sprintf(`labels."run.googleapis.com/execution_name"="%s"`, execution))
		}
	case "worker":
		parts = append(parts, `resource.type="cloud_run_worker_pool"`)
		if name != "" {
			parts = append(parts, fmt.Sprintf(`resource.labels.worker_pool_name="%s"`, name))
		}
		if region != "" {
			parts = append(parts, fmt.Sprintf(`resource.labels.location="%s"`, region))
		}
		parts = append(parts, `NOT log_name:"cloudaudit"`)
	}

	return strings.Join(parts, " AND ")
}

// FetchLogs fetches the last n log entries matching the filter.
// Entries are returned in chronological order (oldest first).
func FetchLogs(ctx context.Context, projectID, filter string, n int) ([]*logging.Entry, error) {
	client, err := logadmin.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("creating logging client: %w", err)
	}
	defer client.Close()

	it := client.Entries(ctx,
		logadmin.Filter(filter),
		logadmin.NewestFirst(),
	)

	var entries []*logging.Entry
	for len(entries) < n {
		entry, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("fetching log entries: %w", err)
		}
		entries = append(entries, entry)
	}

	// Reverse to chronological order (oldest first)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Timestamp.Before(entries[j].Timestamp)
	})

	return entries, nil
}

// StreamLogs streams live log entries to stdout using gRPC TailLogEntries.
// Blocks until ctx is cancelled.
// When fixedPrefix is true, the prefix label is always shown as-is (no label extraction).
func StreamLogs(ctx context.Context, projectID, filter, prefix string, fixedPrefix bool) error {
	tokenSource, err := google.DefaultTokenSource(ctx, "https://www.googleapis.com/auth/logging.read")
	if err != nil {
		return fmt.Errorf("getting credentials: %w", err)
	}

	perRPCCreds := oauth.TokenSource{TokenSource: tokenSource}

	conn, err := grpc.NewClient(
		"logging.googleapis.com:443",
		grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")),
		grpc.WithPerRPCCredentials(perRPCCreds),
	)
	if err != nil {
		return fmt.Errorf("creating gRPC connection: %w", err)
	}
	defer conn.Close()

	client := logpb.NewLoggingServiceV2Client(conn)

	stream, err := client.TailLogEntries(ctx)
	if err != nil {
		return fmt.Errorf("creating tail stream: %w", err)
	}

	req := &logpb.TailLogEntriesRequest{
		ResourceNames: []string{fmt.Sprintf("projects/%s", projectID)},
		Filter:        filter,
		BufferWindow:  durationpb.New(2 * time.Second),
	}
	if err := stream.Send(req); err != nil {
		return fmt.Errorf("sending tail request: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Streaming logs... (Ctrl+C to stop)\n")
	f := NewLogFormatter(prefix, fixedPrefix)
	for {
		resp, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("receiving log entries: %w", err)
		}
		for _, entry := range resp.Entries {
			f.PrintEntry(os.Stdout, protoToEntry(entry))
		}
	}
}

// PrintLogs prints log entries to stdout with an optional prefix label.
// When fixedPrefix is true, the prefix label is always shown as-is (no label extraction).
func PrintLogs(entries []*logging.Entry, prefix string, fixedPrefix bool) {
	f := NewLogFormatter(prefix, fixedPrefix)
	for _, e := range entries {
		f.PrintEntry(os.Stdout, e)
	}
}

// protoToEntry converts a protobuf LogEntry to logging.Entry.
func protoToEntry(entry *logpb.LogEntry) *logging.Entry {
	var payload interface{}
	switch p := entry.Payload.(type) {
	case *logpb.LogEntry_TextPayload:
		payload = p.TextPayload
	case *logpb.LogEntry_JsonPayload:
		if p.JsonPayload != nil {
			payload = p.JsonPayload.AsMap()
		}
	case *logpb.LogEntry_ProtoPayload:
		payload = p.ProtoPayload
	}

	var httpRequest *logging.HTTPRequest
	if req := entry.GetHttpRequest(); req != nil {
		httpRequest = &logging.HTTPRequest{
			Status: int(req.GetStatus()),
		}
		if req.GetRequestMethod() != "" || req.GetRequestUrl() != "" {
			httpRequest.Request = &http.Request{Method: req.GetRequestMethod()}
			if req.GetRequestUrl() != "" {
				if u, err := url.Parse(req.GetRequestUrl()); err == nil {
					httpRequest.Request.URL = u
				}
			}
		}
		if req.GetLatency() != nil {
			httpRequest.Latency = req.GetLatency().AsDuration()
		}
	}

	return &logging.Entry{
		Timestamp:   entry.Timestamp.AsTime(),
		Severity:    logging.Severity(entry.Severity),
		Payload:     payload,
		HTTPRequest: httpRequest,
		Labels:      entry.Labels,
	}
}

// LogFormatter handles colored log output.
type LogFormatter struct {
	useColors    bool
	prefix       string // optional prefix shown before each line, e.g. "legacy-mysql-to-bq"
	useFixedPrefix bool // when true, always use prefix; when false, prefer execution_name label
	prefixColor  *color.Color
	errorColor   *color.Color
	warnColor    *color.Color
	infoColor    *color.Color
	debugColor   *color.Color
}

// NewLogFormatter creates a formatter with TTY-aware color detection.
// prefix is an optional label prepended to every line (empty = no prefix).
// When fixedPrefix is true, the prefix is always shown; when false, the
// run.googleapis.com/execution_name label is preferred over prefix.
func NewLogFormatter(prefix string, fixedPrefix bool) *LogFormatter {
	fileInfo, _ := os.Stdout.Stat()
	useColors := (fileInfo.Mode() & os.ModeCharDevice) != 0
	return &LogFormatter{
		useColors:      useColors,
		prefix:         prefix,
		useFixedPrefix: fixedPrefix,
		prefixColor:    color.New(color.FgHiBlue),
		errorColor:     color.New(color.FgRed),
		warnColor:      color.New(color.FgYellow),
		infoColor:      color.New(color.FgCyan),
		debugColor:     color.New(color.Faint),
	}
}

// PrintEntry formats and prints a single log entry.
func (f *LogFormatter) PrintEntry(w io.Writer, entry *logging.Entry) {
	ts := entry.Timestamp.In(time.Local)
	timeStr := ts.Format("15:04:05.000")

	severityStr := fmt.Sprintf("%-8s", entry.Severity.String())
	if f.useColors {
		switch entry.Severity {
		case logging.Critical, logging.Error:
			severityStr = f.errorColor.Sprint(severityStr)
		case logging.Warning:
			severityStr = f.warnColor.Sprint(severityStr)
		case logging.Info:
			severityStr = f.infoColor.Sprint(severityStr)
		case logging.Debug:
			severityStr = f.debugColor.Sprint(severityStr)
		}
	}

	// Determine label: fixed prefix always wins; otherwise prefer execution_name label.
	var label string
	if f.useFixedPrefix {
		label = f.prefix
	} else {
		label = entry.Labels["run.googleapis.com/execution_name"]
		if label == "" {
			label = f.prefix
		}
	}
	if label != "" {
		pfx := fmt.Sprintf("[%s] ", label)
		if f.useColors {
			pfx = f.prefixColor.Sprint(pfx)
		}
		fmt.Fprintf(w, "%s%s %s %s\n", pfx, timeStr, severityStr, extractLogMessage(entry))
	} else {
		fmt.Fprintf(w, "%s %s %s\n", timeStr, severityStr, extractLogMessage(entry))
	}
}

// extractLogMessage extracts a readable message from a log entry.
func extractLogMessage(entry *logging.Entry) string {
	if entry.HTTPRequest != nil {
		return formatHTTPLogEntry(entry.HTTPRequest)
	}
	if entry.Payload == nil {
		return "[no payload]"
	}
	switch v := entry.Payload.(type) {
	case string:
		return v
	case map[string]interface{}:
		return extractFromLogMap(v)
	}
	return fmt.Sprintf("%v", entry.Payload)
}

// extractFromLogMap extracts a message from a structured map payload.
// If the map looks like slog output (has "msg" or "message" plus extra fields),
// the extra key-value pairs are appended to the message.
func extractFromLogMap(m map[string]interface{}) string {
	// Fields that are never shown as extra key-value pairs.
	metaKeys := map[string]bool{
		"msg": true, "message": true, "text": true,
		"level": true, "severity": true,
		"time": true, "timestamp": true,
		"labels": true, "resource": true, "insertId": true,
	}

	var msgKey string
	for _, key := range []string{"message", "msg", "text"} {
		if v, ok := m[key].(string); ok && v != "" {
			msgKey = key
			_ = v
			break
		}
	}

	if msgKey != "" {
		msg := m[msgKey].(string)
		// Collect remaining structured fields (slog-style extras).
		var extras []string
		for k, v := range m {
			if metaKeys[k] {
				continue
			}
			s := fmt.Sprintf("%v", v)
			if len(s) > 100 {
				s = s[:100] + "..."
			}
			extras = append(extras, fmt.Sprintf("%s=%s", k, s))
		}
		sort.Strings(extras)
		if len(extras) > 0 {
			return msg + "  " + strings.Join(extras, " ")
		}
		return msg
	}

	if httpReq, ok := m["httpRequest"].(map[string]interface{}); ok {
		return formatHTTPLogMap(httpReq)
	}

	// Fallback: format non-noisy fields.
	var parts []string
	for k, v := range m {
		if metaKeys[k] {
			continue
		}
		s := fmt.Sprintf("%v", v)
		if len(s) > 100 {
			s = s[:100] + "..."
		}
		parts = append(parts, fmt.Sprintf("%s=%s", k, s))
	}
	sort.Strings(parts)
	return strings.Join(parts, " ")
}

// formatHTTPLogEntry formats a logging.HTTPRequest.
func formatHTTPLogEntry(req *logging.HTTPRequest) string {
	var parts []string
	if req.Request != nil {
		if req.Request.Method != "" {
			parts = append(parts, req.Request.Method)
		}
		if req.Status > 0 {
			parts = append(parts, fmt.Sprintf("%d", req.Status))
		}
		if req.Request.URL != nil {
			p := req.Request.URL.Path
			if req.Request.URL.RawQuery != "" {
				q := req.Request.URL.RawQuery
				if len(q) > 80 {
					q = q[:80] + "..."
				}
				p = p + "?" + q
			}
			parts = append(parts, p)
		}
	}
	if req.Latency > 0 {
		parts = append(parts, fmt.Sprintf("(%v)", req.Latency))
	}
	if len(parts) > 0 {
		return strings.Join(parts, " ")
	}
	return "[HTTP Request]"
}

// formatHTTPLogMap formats an HTTP request from a map payload.
func formatHTTPLogMap(m map[string]interface{}) string {
	var parts []string
	if method, ok := m["requestMethod"].(string); ok && method != "" {
		parts = append(parts, method)
	}
	if status, ok := m["status"]; ok {
		parts = append(parts, fmt.Sprintf("%v", status))
	}
	if u, ok := m["requestUrl"].(string); ok && u != "" {
		parts = append(parts, u)
	}
	if latency, ok := m["latency"].(string); ok && latency != "" {
		parts = append(parts, fmt.Sprintf("(%s)", latency))
	}
	return strings.Join(parts, " ")
}
