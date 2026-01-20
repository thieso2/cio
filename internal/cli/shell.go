package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/chzyer/readline"
	"github.com/thieso2/cio/bigquery"
	"github.com/thieso2/cio/config"
)

const (
	shellPrompt      = "bq> "
	continuedPrompt  = "  -> "
	historyFileName  = "query_history"
)

// runInteractiveShell starts an interactive BigQuery SQL shell
func runInteractiveShell(ctx context.Context, cfg *config.Config) error {
	// Get project ID
	projectID := cfg.Defaults.ProjectID
	if projectID == "" {
		return fmt.Errorf("project ID not set. Use --project flag or set it in config")
	}

	// Setup history file
	historyFile, err := getHistoryFilePath()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Could not setup history: %v\n", err)
		historyFile = ""
	}

	// Create completer
	completer := createCompleter()

	// Setup readline
	rl, err := readline.NewEx(&readline.Config{
		Prompt:            shellPrompt,
		HistoryFile:       historyFile,
		AutoComplete:      completer,
		InterruptPrompt:   "^C",
		EOFPrompt:         "exit",
		HistorySearchFold: true,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize shell: %w", err)
	}
	defer rl.Close()

	// Print welcome message
	fmt.Println("BigQuery SQL Shell (cio)")
	fmt.Println("Type 'help' for commands, 'exit' or Ctrl+D to quit")
	fmt.Println()

	// REPL loop
	var multilineSQL strings.Builder
	multilineMode := false

	for {
		line, err := rl.Readline()
		if err != nil {
			// EOF or error
			if err == readline.ErrInterrupt {
				if multilineMode {
					// Cancel multiline input
					multilineSQL.Reset()
					multilineMode = false
					rl.SetPrompt(shellPrompt)
					continue
				} else {
					// Exit on Ctrl+C when not in multiline mode
					fmt.Println("\nUse 'exit' or Ctrl+D to quit")
					continue
				}
			}
			// Ctrl+D or other error
			break
		}

		line = strings.TrimSpace(line)

		// Skip empty lines
		if line == "" {
			continue
		}

		// Check for exit commands
		if !multilineMode && (line == "exit" || line == "quit" || line == "\\q") {
			break
		}

		// Check for help
		if !multilineMode && line == "help" {
			printShellHelp()
			continue
		}

		// Check for meta-commands (when not in multiline mode)
		if !multilineMode && strings.HasPrefix(line, "\\") {
			if err := handleMetaCommand(ctx, cfg, projectID, line); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			}
			continue
		}

		// Handle SQL (possibly multiline)
		multilineSQL.WriteString(line)
		multilineSQL.WriteString(" ")

		// Check if statement is complete (ends with ;)
		if strings.HasSuffix(line, ";") {
			// Remove trailing semicolon
			sql := strings.TrimSpace(strings.TrimSuffix(multilineSQL.String(), ";"))
			multilineSQL.Reset()
			multilineMode = false
			rl.SetPrompt(shellPrompt)

			// Execute the query
			if err := executeShellQuery(ctx, cfg, projectID, sql); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			}
			fmt.Println()
		} else {
			// Continue multiline input
			multilineMode = true
			rl.SetPrompt(continuedPrompt)
		}
	}

	fmt.Println("\nGoodbye!")
	return nil
}

// executeShellQuery executes a query in the shell context
func executeShellQuery(ctx context.Context, cfg *config.Config, projectID, sql string) error {
	// Resolve aliases in SQL
	resolvedSQL, err := resolveAliasesInSQL(sql, cfg)
	if err != nil {
		return err
	}

	// Execute query
	result, err := bigquery.ExecuteQuery(ctx, projectID, resolvedSQL, queryMaxResults)
	if err != nil {
		return err
	}

	// Format output (always table in shell)
	if err := bigquery.FormatQueryResultTable(result, os.Stdout); err != nil {
		return err
	}

	// Show statistics
	stats := result.GetStats()
	fmt.Println()
	if stats.CacheHit {
		fmt.Printf("(%d rows in %s, cached)\n",
			stats.RowCount,
			bigquery.FormatDuration(stats.ExecutionTime))
	} else {
		fmt.Printf("(%d rows in %s, %s processed)\n",
			stats.RowCount,
			bigquery.FormatDuration(stats.ExecutionTime),
			bigquery.FormatBytes(stats.BytesProcessed))
	}

	return nil
}

// handleMetaCommand processes shell meta-commands
func handleMetaCommand(ctx context.Context, cfg *config.Config, projectID, cmd string) error {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return nil
	}

	switch parts[0] {
	case "\\d":
		// Describe table
		if len(parts) < 2 {
			return fmt.Errorf("usage: \\d <table>")
		}
		tablePath := parts[1]

		// Resolve alias if needed
		if strings.HasPrefix(tablePath, ":") {
			resolvedPath, err := resolveAliasesInSQL(tablePath, cfg)
			if err != nil {
				return err
			}
			tablePath = strings.TrimPrefix(resolvedPath, "bq://")
		}

		// Split into project.dataset.table
		pathParts := strings.Split(tablePath, ".")
		if len(pathParts) < 2 {
			return fmt.Errorf("invalid table path: %s (expected project.dataset.table or dataset.table)", tablePath)
		}

		var dataset, table string
		if len(pathParts) == 2 {
			dataset = pathParts[0]
			table = pathParts[1]
		} else {
			// Use the last two parts as dataset.table
			dataset = pathParts[len(pathParts)-2]
			table = pathParts[len(pathParts)-1]
		}

		// Describe table
		info, err := bigquery.DescribeTable(ctx, projectID, dataset, table)
		if err != nil {
			return err
		}

		// Display table info
		fmt.Printf("Table: %s.%s\n", dataset, table)
		if info.Description != "" {
			fmt.Printf("Description: %s\n", info.Description)
		}
		fmt.Printf("Created: %s\n", info.Created.Format("2006-01-02 15:04:05"))
		fmt.Printf("Modified: %s\n", info.Modified.Format("2006-01-02 15:04:05"))
		fmt.Printf("Location: %s\n", info.Location)
		fmt.Printf("Size: %s\n", bigquery.FormatBytes(info.SizeBytes))
		fmt.Printf("Rows: %d\n", info.NumRows)
		fmt.Println()
		fmt.Println("Schema:")
		bigquery.PrintSchema(info.Schema, 0)

	case "\\l":
		// List tables
		// For now, show a helpful message
		fmt.Println("Use: SELECT table_name FROM `project.dataset.INFORMATION_SCHEMA.TABLES`")
		fmt.Println("Or use 'cio ls :alias' outside the shell")

	case "\\q":
		// Quit (handled in main loop)
		return nil

	default:
		return fmt.Errorf("unknown meta-command: %s", parts[0])
	}

	return nil
}

// printShellHelp displays help for the interactive shell
func printShellHelp() {
	fmt.Println("BigQuery SQL Shell Commands:")
	fmt.Println()
	fmt.Println("SQL Queries:")
	fmt.Println("  Type SQL queries and end with ; to execute")
	fmt.Println("  Multi-line queries are supported")
	fmt.Println("  Use :alias syntax for mapped datasets/tables")
	fmt.Println()
	fmt.Println("Meta-commands:")
	fmt.Println("  \\d <table>    Describe table schema")
	fmt.Println("  \\l            List tables (shows hint)")
	fmt.Println("  \\q            Quit shell")
	fmt.Println()
	fmt.Println("Shell commands:")
	fmt.Println("  help          Show this help")
	fmt.Println("  exit          Exit shell")
	fmt.Println("  quit          Exit shell")
	fmt.Println()
	fmt.Println("Keyboard shortcuts:")
	fmt.Println("  Ctrl+C        Cancel current input")
	fmt.Println("  Ctrl+D        Exit shell")
	fmt.Println("  Up/Down       Navigate history")
	fmt.Println()
}

// createCompleter creates an autocompleter for SQL keywords
func createCompleter() *readline.PrefixCompleter {
	return readline.NewPrefixCompleter(
		// SQL keywords
		readline.PcItem("SELECT"),
		readline.PcItem("FROM"),
		readline.PcItem("WHERE"),
		readline.PcItem("JOIN"),
		readline.PcItem("LEFT"),
		readline.PcItem("RIGHT"),
		readline.PcItem("INNER"),
		readline.PcItem("OUTER"),
		readline.PcItem("ON"),
		readline.PcItem("GROUP"),
		readline.PcItem("ORDER"),
		readline.PcItem("BY"),
		readline.PcItem("HAVING"),
		readline.PcItem("LIMIT"),
		readline.PcItem("OFFSET"),
		readline.PcItem("AS"),
		readline.PcItem("AND"),
		readline.PcItem("OR"),
		readline.PcItem("NOT"),
		readline.PcItem("IN"),
		readline.PcItem("EXISTS"),
		readline.PcItem("BETWEEN"),
		readline.PcItem("LIKE"),
		readline.PcItem("IS"),
		readline.PcItem("NULL"),
		readline.PcItem("COUNT"),
		readline.PcItem("SUM"),
		readline.PcItem("AVG"),
		readline.PcItem("MIN"),
		readline.PcItem("MAX"),
		readline.PcItem("DISTINCT"),
		readline.PcItem("ASC"),
		readline.PcItem("DESC"),
		readline.PcItem("INSERT"),
		readline.PcItem("UPDATE"),
		readline.PcItem("DELETE"),
		readline.PcItem("CREATE"),
		readline.PcItem("DROP"),
		readline.PcItem("ALTER"),
		readline.PcItem("TABLE"),
		readline.PcItem("VIEW"),
		readline.PcItem("INDEX"),

		// Meta-commands
		readline.PcItem("\\d"),
		readline.PcItem("\\l"),
		readline.PcItem("\\q"),

		// Shell commands
		readline.PcItem("help"),
		readline.PcItem("exit"),
		readline.PcItem("quit"),
	)
}

// getHistoryFilePath returns the path to the history file
func getHistoryFilePath() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	configDir := filepath.Join(homeDir, ".config", "cio")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return "", err
	}

	return filepath.Join(configDir, historyFileName), nil
}
