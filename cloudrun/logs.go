package cloudrun

import (
	"fmt"
	"strings"
)

// This file builds Cloud Logging filter strings for Cloud Run resources. The
// fetch/stream/format machinery lives in internal/logtail; only the
// resource-specific filter construction stays here.

// LogFilterMultiJob builds a Cloud Logging filter for multiple job names (OR-joined).
// Used when a wildcard pattern is expanded into concrete job names before filtering.
func LogFilterMultiJob(region string, jobNames []string, execution string, audit bool, severity string) string {
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
	if audit {
		parts = append(parts, `log_name:"cloudaudit"`)
	} else {
		switch execution {
		case "", "*":
			parts = append(parts, `labels."run.googleapis.com/execution_name":*`)
			parts = append(parts, `NOT log_name:"cloudaudit"`)
		default:
			parts = append(parts, fmt.Sprintf(`labels."run.googleapis.com/execution_name"="%s"`, execution))
		}
	}
	if severity != "" {
		parts = append(parts, fmt.Sprintf(`severity>=%s`, strings.ToUpper(severity)))
	}
	return strings.Join(parts, " AND ")
}

// LogFilter builds a Cloud Logging filter for a Cloud Run resource.
//
// For jobs, the execution argument controls scope:
//
//	""         – all execution logs (any execution), excludes audit logs; prefix = job name
//	"*"        – all execution logs (any execution), excludes audit logs; prefix = execution label
//	"<id>"     – logs for one specific execution
//
// When audit is true, returns Cloud Audit logs instead (job-level events like
// "execution created", "job updated"). Audit logs exist at the job resource level
// and do not carry execution labels.
func LogFilter(projectID, region, scheme, name, execution string, audit bool, severity string) string {
	var parts []string

	switch scheme {
	case "svc":
		parts = append(parts, `resource.type="cloud_run_revision"`)
		if name != "" {
			parts = append(parts, fmt.Sprintf(`resource.labels.service_name="%s"`, name))
		}
		if region != "" {
			parts = append(parts, fmt.Sprintf(`resource.labels.location="%s"`, region))
		}
		if !audit {
			parts = append(parts, `NOT log_name:"cloudaudit"`)
		} else {
			parts = append(parts, `log_name:"cloudaudit"`)
		}
	case "jobs":
		parts = append(parts, `resource.type="cloud_run_job"`)
		if name != "" {
			parts = append(parts, fmt.Sprintf(`resource.labels.job_name="%s"`, name))
		}
		if region != "" {
			parts = append(parts, fmt.Sprintf(`resource.labels.location="%s"`, region))
		}
		if audit {
			// Audit logs: job-level events (execution created, job updated, etc.)
			// They do NOT have execution_name labels — they're at the job resource level.
			parts = append(parts, `log_name:"cloudaudit"`)
		} else {
			switch execution {
			case "", "*":
				// All executions (no specific execution given, or explicit wildcard).
				// Exclude audit noise; execution label must be present.
				parts = append(parts, `labels."run.googleapis.com/execution_name":*`)
				parts = append(parts, `NOT log_name:"cloudaudit"`)
			default:
				// Specific execution
				parts = append(parts, fmt.Sprintf(`labels."run.googleapis.com/execution_name"="%s"`, execution))
			}
		}
	case "worker":
		parts = append(parts, `resource.type="cloud_run_worker_pool"`)
		if name != "" {
			parts = append(parts, fmt.Sprintf(`resource.labels.worker_pool_name="%s"`, name))
		}
		if region != "" {
			parts = append(parts, fmt.Sprintf(`resource.labels.location="%s"`, region))
		}
		if !audit {
			parts = append(parts, `NOT log_name:"cloudaudit"`)
		} else {
			parts = append(parts, `log_name:"cloudaudit"`)
		}
	}

	if severity != "" {
		parts = append(parts, fmt.Sprintf(`severity>=%s`, strings.ToUpper(severity)))
	}

	return strings.Join(parts, " AND ")
}

// PerJobFilters returns one filter per job name, for fetching N entries per job.
func PerJobFilters(projectID, region string, jobNames []string, execution string, audit bool, severity string) []string {
	filters := make([]string, len(jobNames))
	for i, name := range jobNames {
		filters[i] = LogFilter(projectID, region, "jobs", name, execution, audit, severity)
	}
	return filters
}
