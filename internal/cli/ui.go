package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/internal/web"
)

var (
	uiPort   int
	uiHost   string
	uiNoOpen bool
)

var uiCmd = &cobra.Command{
	Use:   "ui",
	Short: "Start web UI for browsing GCP resources",
	Long: `Start a web server that provides a modern web UI for browsing
GCS buckets, BigQuery datasets and tables, and IAM service accounts.

The UI provides:
- Browse GCS buckets and objects
- Browse BigQuery datasets and tables
- Browse IAM service accounts
- Search and filter resources
- Preview text files and images
- Grid and list view modes
- Dark mode support

The browser will automatically open unless --no-open is specified.`,
	Example: `  # Start UI on default port (8080)
  cio ui

  # Start UI on custom port
  cio ui --port 3000

  # Start UI without auto-opening browser
  cio ui --no-open

  # Start UI on all interfaces
  cio ui --host 0.0.0.0`,
	RunE: runUI,
}

func init() {
	uiCmd.Flags().IntVarP(&uiPort, "port", "p", 0, "Server port (default from config or 8080)")
	uiCmd.Flags().StringVar(&uiHost, "host", "", "Server host (default from config or localhost)")
	uiCmd.Flags().BoolVar(&uiNoOpen, "no-open", false, "Don't automatically open browser")

	rootCmd.AddCommand(uiCmd)
}

func runUI(cmd *cobra.Command, args []string) error {
	cfg := GetConfig()

	// Override config with CLI flags if provided
	if uiPort > 0 {
		cfg.Server.Port = uiPort
	}
	if uiHost != "" {
		cfg.Server.Host = uiHost
	}
	if uiNoOpen {
		cfg.Server.AutoStart = false
	}

	// Set defaults if not configured
	if cfg.Server.Port == 0 {
		cfg.Server.Port = 8080
	}
	if cfg.Server.Host == "" {
		cfg.Server.Host = "localhost"
	}

	// Create server
	server, err := web.New(cfg)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	// Start server in goroutine
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.Start()
	}()

	// Auto-open browser if configured
	if cfg.Server.AutoStart && !uiNoOpen {
		// Give server time to start
		time.Sleep(500 * time.Millisecond)

		url := fmt.Sprintf("http://%s:%d", cfg.Server.Host, cfg.Server.Port)
		if err := web.OpenBrowser(url); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to open browser: %v\n", err)
			fmt.Printf("Please open %s in your browser\n", url)
		}
	}

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for shutdown signal or server error
	select {
	case err := <-serverErr:
		if err != nil {
			return fmt.Errorf("server error: %w", err)
		}
	case sig := <-sigChan:
		fmt.Printf("\nReceived signal %v, shutting down gracefully...\n", sig)

		// Create shutdown context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Shutdown server
		if err := server.Shutdown(ctx); err != nil {
			return fmt.Errorf("server shutdown error: %w", err)
		}

		fmt.Println("Server stopped")
	}

	return nil
}
