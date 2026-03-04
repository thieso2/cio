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
	"google.golang.org/protobuf/types/known/structpb"
)

// LogFilterMultiJob builds a Cloud Logging filter for multiple job names (OR-joined).
// Used when a wildcard pattern is expanded into concrete job names before filtering.
func LogFilterMultiJob(region string, jobNames []string, execution string) string {
	var parts []string
	parts = append(parts, `resource.type="cloud_run_job"`)
	if region != "" {
		parts = append(parts, fmt.Sprintf(`resource.labels.location="%s"`, region))
	}
	if len(jobNames) == 1 {
		parts = append(parts, fmt.Sprintf(`resource.labels.job_name="%s"`, jobNames[0]))
	} else if len(jobNames) > 1 {
		var nameFilters []string
		for _, n := range jobNames {
			nameFilters = append(nameFilters, fmt.Sprintf(`resource.labels.job_name="%s"`, n))
		}
		parts = append(parts, "("+strings.Join(nameFilters, " OR ")+")")
	}
	switch execution {
	case "":
		parts = append(parts, `NOT labels."run.googleapis.com/execution_name":*`)
		parts = append(parts, `NOT log_name:"cloudaudit"`)
	case "*":
		parts = append(parts, `labels."run.googleapis.com/execution_name":*`)
	default:
		parts = append(parts, fmt.Sprintf(`labels."run.googleapis.com/execution_name"="%s"`, execution))
	}
	return strings.Join(parts, " AND ")
}

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

// FetchLogsMultiJob fetches n log entries per job name, merges, and returns
// them in chronological order (oldest first).
func FetchLogsMultiJob(ctx context.Context, projectID, region string, jobNames []string, execution string, n int) ([]*logging.Entry, error) {
	var all []*logging.Entry
	for _, jobName := range jobNames {
		f := LogFilter(projectID, region, "jobs", jobName, execution)
		entries, err := FetchLogs(ctx, projectID, f, n)
		if err != nil {
			return nil, fmt.Errorf("fetching logs for job %s: %w", jobName, err)
		}
		all = append(all, entries...)
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].Timestamp.Before(all[j].Timestamp)
	})
	return all, nil
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

// prefixPalette is the cycling set of colors assigned to distinct prefix labels.
var prefixPalette = []*color.Color{
	color.New(color.FgHiBlue),
	color.New(color.FgHiGreen),
	color.New(color.FgHiMagenta),
	color.New(color.FgHiCyan),
	color.New(color.FgHiYellow),
	color.New(color.FgBlue),
	color.New(color.FgGreen),
	color.New(color.FgMagenta),
	color.New(color.FgCyan),
	color.New(color.FgYellow),
}

// LogFormatter handles colored log output.
type LogFormatter struct {
	useColors      bool
	prefix         string // optional prefix shown before each line, e.g. "legacy-mysql-to-bq"
	useFixedPrefix bool   // when true, always use prefix; when false, prefer execution_name label
	timeColor      *color.Color
	errorColor     *color.Color
	warnColor      *color.Color
	infoColor      *color.Color
	debugColor     *color.Color
	keyColor       *color.Color // slog key names
	prefixColorMap map[string]*color.Color // label → assigned palette color
	prefixColorIdx int
}

// NewLogFormatter creates a formatter with TTY-aware color detection.
// prefix is an optional label prepended to every line (empty = no prefix).
// When fixedPrefix is true, the prefix is always shown; when false, the
// run.googleapis.com/execution_name label is preferred over prefix.
func NewLogFormatter(prefix string, fixedPrefix bool) *LogFormatter {
	fileInfo, _ := os.Stdout.Stat()
	useColors := (fileInfo.Mode() & os.ModeCharDevice) != 0
	if useColors {
		// fatih/color may disagree with our isatty check; override it.
		color.NoColor = false
	}
	return &LogFormatter{
		useColors:      useColors,
		prefix:         prefix,
		useFixedPrefix: fixedPrefix,
		timeColor:      color.New(color.Faint),
		errorColor:     color.New(color.FgRed, color.Bold),
		warnColor:      color.New(color.FgYellow),
		infoColor:      color.New(color.FgCyan),
		debugColor:     color.New(color.Faint),
		keyColor:       color.New(color.Faint),
		prefixColorMap: make(map[string]*color.Color),
	}
}

// prefixColor returns a stable color for the given label, cycling through the palette.
func (f *LogFormatter) prefixColor(label string) *color.Color {
	if c, ok := f.prefixColorMap[label]; ok {
		return c
	}
	c := prefixPalette[f.prefixColorIdx%len(prefixPalette)]
	f.prefixColorIdx++
	f.prefixColorMap[label] = c
	return c
}

// PrintEntry formats and prints a single log entry.
func (f *LogFormatter) PrintEntry(w io.Writer, entry *logging.Entry) {
	ts := entry.Timestamp.In(time.Local)
	timeStr := ts.Format("15:04:05.000")
	if f.useColors {
		timeStr = f.timeColor.Sprint(timeStr)
	}

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
			pfx = f.prefixColor(label).Sprint(pfx)
		}
		fmt.Fprintf(w, "%s%s %s %s\n", pfx, timeStr, severityStr, f.formatMessage(entry))
	} else {
		fmt.Fprintf(w, "%s %s %s\n", timeStr, severityStr, f.formatMessage(entry))
	}
}

// formatMessage extracts and formats a readable message from a log entry.
func (f *LogFormatter) formatMessage(entry *logging.Entry) string {
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
		return f.formatLogMap(v)
	case *structpb.Struct:
		// logadmin returns JSON payloads as *structpb.Struct; convert to map first.
		return f.formatLogMap(v.AsMap())
	}
	return fmt.Sprintf("%v", entry.Payload)
}

// formatLogMap formats a structured map payload.
// If a msg/message key is present, extra fields are appended as key=value pairs (slog style).
func (f *LogFormatter) formatLogMap(m map[string]interface{}) string {
	// Fields never shown as extra key-value pairs.
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
			break
		}
	}

	if msgKey != "" {
		msg := m[msgKey].(string)
		// Collect remaining structured fields (slog-style extras).
		var keys []string
		for k := range m {
			if !metaKeys[k] {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		var extras []string
		for _, k := range keys {
			s := fmt.Sprintf("%v", m[k])
			if len(s) > 100 {
				s = s[:100] + "..."
			}
			if f.useColors {
				extras = append(extras, f.keyColor.Sprint(k+"=")+s)
			} else {
				extras = append(extras, k+"="+s)
			}
		}
		if len(extras) > 0 {
			return msg + "  " + strings.Join(extras, " ")
		}
		return msg
	}

	if httpReq, ok := m["httpRequest"].(map[string]interface{}); ok {
		return formatHTTPLogMap(httpReq)
	}

	// Fallback: format all non-meta fields.
	var keys []string
	for k := range m {
		if !metaKeys[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		s := fmt.Sprintf("%v", m[k])
		if len(s) > 100 {
			s = s[:100] + "..."
		}
		if f.useColors {
			parts = append(parts, f.keyColor.Sprint(k+"=")+s)
		} else {
			parts = append(parts, k+"="+s)
		}
	}
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
