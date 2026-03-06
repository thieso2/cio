package dataflow

import (
	"context"
	"fmt"
	"io"
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

// LogType represents which Dataflow logs to show.
type LogType string

const (
	LogTypeAll    LogType = "all"
	LogTypeJob    LogType = "job"
	LogTypeWorker LogType = "worker"
	LogTypeStep   LogType = "step"
)

// ValidLogTypes returns all valid log type values.
func ValidLogTypes() []string {
	return []string{string(LogTypeAll), string(LogTypeJob), string(LogTypeWorker), string(LogTypeStep)}
}

// logFilter builds a Cloud Logging filter for Dataflow logs.
func logFilter(projectID, jobID string, logType LogType, severity string) string {
	base := fmt.Sprintf(`resource.type="dataflow_step" AND resource.labels.job_id="%s"`, jobID)

	var logNames []string
	switch logType {
	case LogTypeJob:
		logNames = []string{
			fmt.Sprintf(`"projects/%s/logs/dataflow.googleapis.com%%2Fjob-message"`, projectID),
			fmt.Sprintf(`"projects/%s/logs/dataflow.googleapis.com%%2Flauncher"`, projectID),
		}
	case LogTypeWorker:
		logNames = []string{
			fmt.Sprintf(`"projects/%s/logs/dataflow.googleapis.com%%2Fworker-startup"`, projectID),
			fmt.Sprintf(`"projects/%s/logs/dataflow.googleapis.com%%2Fharness"`, projectID),
			fmt.Sprintf(`"projects/%s/logs/dataflow.googleapis.com%%2Fharness-startup"`, projectID),
		}
	case LogTypeStep:
		logNames = []string{
			fmt.Sprintf(`"projects/%s/logs/dataflow.googleapis.com%%2Fworker"`, projectID),
		}
	case LogTypeAll:
		// No logName filter — fetch all Dataflow logs for this job.
	}

	filter := base
	if len(logNames) == 1 {
		filter += " AND logName=" + logNames[0]
	} else if len(logNames) > 1 {
		filter += " AND logName=(" + strings.Join(logNames, " OR ") + ")"
	}

	if severity != "" {
		filter += fmt.Sprintf(` AND severity>=%s`, strings.ToUpper(severity))
	}
	return filter
}

// LogFilters returns a map of log type → filter string.
// For "all" mode, returns separate filters for J/W/S so entries can be tagged.
func LogFilters(projectID, jobID string, logType LogType, severity string) map[LogType]string {
	if logType != LogTypeAll {
		return map[LogType]string{
			logType: logFilter(projectID, jobID, logType, severity),
		}
	}
	return map[LogType]string{
		LogTypeJob:    logFilter(projectID, jobID, LogTypeJob, severity),
		LogTypeWorker: logFilter(projectID, jobID, LogTypeWorker, severity),
		LogTypeStep:   logFilter(projectID, jobID, LogTypeStep, severity),
	}
}

// taggedEntry wraps a logging.Entry with its source log type.
type taggedEntry struct {
	Entry   *logging.Entry
	LogType LogType
}

// FetchLogs fetches the last n log entries for a Dataflow job.
// Returns entries tagged with their log type.
func FetchLogs(ctx context.Context, projectID, jobID string, logType LogType, severity string, n int) ([]taggedEntry, error) {
	filters := LogFilters(projectID, jobID, logType, severity)

	client, err := logadmin.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("creating logging client: %w", err)
	}
	defer client.Close()

	var all []taggedEntry
	for lt, filter := range filters {
		it := client.Entries(ctx,
			logadmin.Filter(filter),
			logadmin.NewestFirst(),
		)
		var entries []taggedEntry
		for len(entries) < n {
			entry, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("fetching %s log entries: %w", lt, err)
			}
			entries = append(entries, taggedEntry{Entry: entry, LogType: lt})
		}
		all = append(all, entries...)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Entry.Timestamp.Before(all[j].Entry.Timestamp)
	})
	return all, nil
}

// StreamLogs streams live Dataflow log entries to stdout using gRPC TailLogEntries.
func StreamLogs(ctx context.Context, projectID, jobID string, logType LogType, severity string, f *LogFormatter) error {
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
	filters := LogFilters(projectID, jobID, logType, severity)

	// For "all" mode, we open one stream per log type to tag entries correctly.
	// For single type, just one stream.
	type streamInfo struct {
		stream  logpb.LoggingServiceV2_TailLogEntriesClient
		req     *logpb.TailLogEntriesRequest
		logType LogType
	}

	var streams []streamInfo
	for lt, filter := range filters {
		stream, err := client.TailLogEntries(ctx)
		if err != nil {
			return fmt.Errorf("creating tail stream for %s: %w", lt, err)
		}
		req := &logpb.TailLogEntriesRequest{
			ResourceNames: []string{fmt.Sprintf("projects/%s", projectID)},
			Filter:        filter,
			BufferWindow:  durationpb.New(2 * time.Second),
		}
		if err := stream.Send(req); err != nil {
			return fmt.Errorf("sending tail request for %s: %w", lt, err)
		}
		streams = append(streams, streamInfo{stream: stream, req: req, logType: lt})
	}

	fmt.Fprintf(os.Stderr, "Streaming logs... (Ctrl+C to stop)\n")

	// For single stream, just read from it directly.
	if len(streams) == 1 {
		s := streams[0]
		const maxRetries = 2
		for {
			resp, err := s.stream.Recv()
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				reconnected := false
				for attempt := 1; attempt <= maxRetries; attempt++ {
					time.Sleep(time.Duration(attempt) * time.Second)
					if ctx.Err() != nil {
						return nil
					}
					newStream, sendErr := client.TailLogEntries(ctx)
					if sendErr != nil {
						continue
					}
					if sendErr = newStream.Send(s.req); sendErr != nil {
						continue
					}
					s.stream = newStream
					reconnected = true
					break
				}
				if !reconnected {
					return fmt.Errorf("receiving log entries: %w", err)
				}
				continue
			}
			for _, entry := range resp.Entries {
				f.PrintEntry(os.Stdout, protoToEntry(entry), s.logType)
			}
		}
	}

	// Multiple streams: read from each in separate goroutines, merge via channel.
	type taggedProtoEntry struct {
		entry   *logpb.LogEntry
		logType LogType
	}
	ch := make(chan taggedProtoEntry, 100)
	errCh := make(chan error, len(streams))

	for _, s := range streams {
		go func(si streamInfo) {
			for {
				resp, err := si.stream.Recv()
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					errCh <- fmt.Errorf("receiving %s log entries: %w", si.logType, err)
					return
				}
				for _, entry := range resp.Entries {
					ch <- taggedProtoEntry{entry: entry, logType: si.logType}
				}
			}
		}(s)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			return err
		case te := <-ch:
			f.PrintEntry(os.Stdout, protoToEntry(te.entry), te.logType)
		}
	}
}

// PrintLogs prints tagged log entries using the formatter.
func PrintLogs(entries []taggedEntry, f *LogFormatter) {
	for _, te := range entries {
		f.PrintEntry(os.Stdout, te.Entry, te.LogType)
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

	e := &logging.Entry{
		Timestamp: entry.Timestamp.AsTime(),
		Severity:  logging.Severity(entry.Severity),
		Payload:   payload,
		Labels:    entry.Labels,
	}
	if entry.Resource != nil {
		e.Resource = entry.Resource
	}
	return e
}

// LogFormatter handles colored log output for Dataflow logs.
type LogFormatter struct {
	useColors  bool
	showPrefix bool // show [J]/[W]/[S] prefix in "all" mode
	timeColor  *color.Color
	errorColor *color.Color
	warnColor  *color.Color
	infoColor  *color.Color
	debugColor *color.Color
	keyColor   *color.Color
	jobColor   *color.Color
	workerColor *color.Color
	stepColor  *color.Color
}

// NewLogFormatter creates a formatter with TTY-aware color detection.
func NewLogFormatter(showPrefix bool) *LogFormatter {
	fileInfo, _ := os.Stdout.Stat()
	useColors := (fileInfo.Mode() & os.ModeCharDevice) != 0
	if useColors {
		color.NoColor = false
	}
	return &LogFormatter{
		useColors:   useColors,
		showPrefix:  showPrefix,
		timeColor:   color.New(color.Faint),
		errorColor:  color.New(color.FgRed, color.Bold),
		warnColor:   color.New(color.FgYellow),
		infoColor:   color.New(color.FgCyan),
		debugColor:  color.New(color.Faint),
		keyColor:    color.New(color.Faint),
		jobColor:    color.New(color.FgHiBlue),
		workerColor: color.New(color.FgHiMagenta),
		stepColor:   color.New(color.FgHiGreen),
	}
}

// logTypeTag returns the short prefix tag for a log type.
func logTypeTag(lt LogType) string {
	switch lt {
	case LogTypeJob:
		return "[J]"
	case LogTypeWorker:
		return "[W]"
	case LogTypeStep:
		return "[S]"
	default:
		return ""
	}
}

// PrintEntry formats and prints a single Dataflow log entry.
func (f *LogFormatter) PrintEntry(w io.Writer, entry *logging.Entry, lt LogType) {
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

	var prefix string
	if f.showPrefix {
		tag := logTypeTag(lt)
		if f.useColors && tag != "" {
			switch lt {
			case LogTypeJob:
				tag = f.jobColor.Sprint(tag)
			case LogTypeWorker:
				tag = f.workerColor.Sprint(tag)
			case LogTypeStep:
				tag = f.stepColor.Sprint(tag)
			}
		}
		if tag != "" {
			prefix = tag + " "
		}
	}

	msg := f.formatMessage(entry)
	fmt.Fprintf(w, "%s%s %s %s\n", prefix, timeStr, severityStr, msg)
}

// formatMessage extracts a readable message from a log entry.
func (f *LogFormatter) formatMessage(entry *logging.Entry) string {
	if entry.Payload == nil {
		return "[no payload]"
	}
	switch v := entry.Payload.(type) {
	case string:
		return v
	case map[string]interface{}:
		return f.formatLogMap(v)
	case *structpb.Struct:
		return f.formatLogMap(v.AsMap())
	}
	return fmt.Sprintf("%v", entry.Payload)
}

// formatLogMap formats a structured map payload.
func (f *LogFormatter) formatLogMap(m map[string]interface{}) string {
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
