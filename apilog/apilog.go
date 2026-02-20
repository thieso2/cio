// Package apilog provides lightweight verbose logging for GCP API calls.
// Set Verbose = true (via --verbose flag or VERBOSE=1 env var) to enable.
package apilog

import (
	"fmt"
	"os"
)

// Verbose controls whether API call traces are written to stderr.
var Verbose bool

// Logf writes a single API call trace line to stderr when Verbose is true.
// The caller supplies a format string describing the specific API call,
// e.g. apilog.Logf("[GCS] Objects.List(bucket=%s, prefix=%q)", bucket, pfx)
func Logf(format string, args ...any) {
	if Verbose {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
}
