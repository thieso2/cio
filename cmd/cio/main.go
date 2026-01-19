package main

import (
	"fmt"
	"os"

	"github.com/thieso2/cio/internal/cli"
)

// Version information (set by ldflags during build)
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
	builtBy = "unknown"
)

func main() {
	// Set version info in CLI
	cli.SetVersionInfo(version, commit, date, builtBy)

	if err := cli.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
