package dataflow

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	"github.com/fatih/color"
	"github.com/thieso2/cio/internal/logtail"
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

// orderedFilters returns the filters and a parallel slice of their log types, in
// a stable order, for the given selection. The slice index is what logtail tags
// each entry with so we can recover the LogType.
func orderedFilters(projectID, jobID string, logType LogType, severity string) ([]string, []LogType) {
	if logType != LogTypeAll {
		return []string{logFilter(projectID, jobID, logType, severity)}, []LogType{logType}
	}
	types := []LogType{LogTypeJob, LogTypeWorker, LogTypeStep}
	filters := make([]string, len(types))
	for i, lt := range types {
		filters[i] = logFilter(projectID, jobID, lt, severity)
	}
	return filters, types
}

// taggedEntry wraps a logging.Entry with its source log type.
type taggedEntry struct {
	Entry   *logging.Entry
	LogType LogType
}

// FetchLogs fetches the last n log entries for a Dataflow job, tagged with their
// log type. Delegates the logadmin work to logtail.Fetch.
func FetchLogs(ctx context.Context, projectID, jobID string, logType LogType, severity string, n int) ([]taggedEntry, error) {
	filters, types := orderedFilters(projectID, jobID, logType, severity)
	tagged, err := logtail.Fetch(ctx, projectID, filters, n)
	if err != nil {
		return nil, err
	}
	out := make([]taggedEntry, len(tagged))
	for i, t := range tagged {
		out[i] = taggedEntry{Entry: t.Entry, LogType: types[t.Filter]}
	}
	return out, nil
}

// StreamLogs streams live Dataflow log entries, tagging each by its source
// filter's log type. Delegates the gRPC tailing to logtail.Stream.
func StreamLogs(ctx context.Context, projectID, jobID string, logType LogType, severity string, f *LogFormatter) error {
	filters, types := orderedFilters(projectID, jobID, logType, severity)
	return logtail.Stream(ctx, projectID, filters, func(e *logging.Entry, idx int) {
		f.PrintEntry(os.Stdout, e, types[idx])
	})
}

// PrintLogs prints tagged log entries using the formatter.
func PrintLogs(entries []taggedEntry, f *LogFormatter) {
	for _, te := range entries {
		f.PrintEntry(os.Stdout, te.Entry, te.LogType)
	}
}

// LogFormatter handles colored log output for Dataflow logs, prefixing each line
// with a [J]/[W]/[S] tag in "all" mode. The structured-payload formatting is
// shared with the default formatter via logtail.FormatLogMap.
type LogFormatter struct {
	useColors   bool
	showPrefix  bool // show [J]/[W]/[S] prefix in "all" mode
	timeColor   *color.Color
	errorColor  *color.Color
	warnColor   *color.Color
	infoColor   *color.Color
	debugColor  *color.Color
	keyColor    *color.Color
	jobColor    *color.Color
	workerColor *color.Color
	stepColor   *color.Color
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
		return logtail.FormatLogMap(v, f.useColors, f.keyColor)
	case *structpb.Struct:
		return logtail.FormatLogMap(v.AsMap(), f.useColors, f.keyColor)
	}
	return fmt.Sprintf("%v", entry.Payload)
}
