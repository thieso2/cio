package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/idtoken"
	"google.golang.org/api/option"
)

var (
	authCredentials string
	authAudience    string
)

// authCmd represents the auth command
var authCmd = &cobra.Command{
	Use:   "auth",
	Short: "Authentication commands",
	Long: `Authentication commands for managing GCP credentials and tokens.

Examples:
  # Print access token using ADC
  cio auth print-access-token

  # Print access token using service account
  cio auth print-access-token -c /path/to/service-account.json

  # Print identity token for a specific audience
  cio auth print-identity-token -a https://example-service.run.app

  # Print identity token using service account
  cio auth print-identity-token -a https://example.com -c /path/to/sa.json`,
}

var printAccessTokenCmd = &cobra.Command{
	Use:   "print-access-token",
	Short: "Print an access token for the current credentials",
	Long: `Print an access token for the current credentials.

This command is similar to 'gcloud auth print-access-token' and prints
a valid OAuth 2.0 access token that can be used to authenticate with
Google Cloud APIs.

By default, it uses Application Default Credentials (ADC). You can override
this by providing a service account JSON file with the -c flag.

Examples:
  # Using ADC
  cio auth print-access-token

  # Using service account
  cio auth print-access-token -c /path/to/service-account.json

  # Use in curl command
  curl -H "Authorization: Bearer $(cio auth print-access-token)" \
    https://storage.googleapis.com/storage/v1/b`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()

		var creds *google.Credentials
		var err error

		if authCredentials != "" {
			// Load credentials from file
			data, err := os.ReadFile(authCredentials)
			if err != nil {
				return fmt.Errorf("failed to read credentials file: %w", err)
			}
			creds, err = google.CredentialsFromJSON(ctx, data, "https://www.googleapis.com/auth/cloud-platform")
			if err != nil {
				return fmt.Errorf("failed to parse credentials: %w", err)
			}
		} else {
			// Use ADC
			creds, err = google.FindDefaultCredentials(ctx, "https://www.googleapis.com/auth/cloud-platform")
			if err != nil {
				return fmt.Errorf("failed to get default credentials: %w", err)
			}
		}

		// Get token
		token, err := creds.TokenSource.Token()
		if err != nil {
			return fmt.Errorf("failed to get access token: %w", err)
		}

		// Print token
		fmt.Println(token.AccessToken)
		return nil
	},
}

var printIdentityTokenCmd = &cobra.Command{
	Use:   "print-identity-token",
	Short: "Print an identity token for the current credentials",
	Long: `Print an identity token for the current credentials.

This command is similar to 'gcloud auth print-identity-token' and prints
a valid OpenID Connect (OIDC) identity token that can be used to authenticate
with services that require identity tokens (e.g., Cloud Run, Cloud Functions).

The -a/--audience flag is REQUIRED and specifies the target audience URL.

By default, it uses Application Default Credentials (ADC). You can override
this by providing a service account JSON file with the -c flag.

Examples:
  # Using ADC with Cloud Run service
  cio auth print-identity-token -a https://my-service-abc123.run.app

  # Using service account
  cio auth print-identity-token \
    -a https://my-service.run.app \
    -c /path/to/service-account.json

  # Use in curl command
  curl -H "Authorization: Bearer $(cio auth print-identity-token -a https://my-service.run.app)" \
    https://my-service-abc123.run.app`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if authAudience == "" {
			return fmt.Errorf("audience is required (use -a or --audience flag)")
		}

		ctx := context.Background()

		var ts oauth2.TokenSource
		var err error

		if authCredentials != "" {
			// Use service account credentials
			ts, err = idtoken.NewTokenSource(ctx, authAudience, option.WithCredentialsFile(authCredentials))
		} else {
			// Use ADC
			ts, err = idtoken.NewTokenSource(ctx, authAudience)
		}

		if err != nil {
			return fmt.Errorf("failed to create token source: %w", err)
		}

		// Get token
		token, err := ts.Token()
		if err != nil {
			return fmt.Errorf("failed to get identity token: %w", err)
		}

		// Print token
		fmt.Println(token.AccessToken)
		return nil
	},
}

func init() {
	// Add auth command flags
	authCmd.PersistentFlags().StringVarP(&authCredentials, "credentials", "c", "", "Path to service account JSON file")

	// Add print-identity-token flags
	printIdentityTokenCmd.Flags().StringVarP(&authAudience, "audience", "a", "", "Target audience URL (e.g., https://example-service.run.app)")

	// Add subcommands
	authCmd.AddCommand(printAccessTokenCmd)
	authCmd.AddCommand(printIdentityTokenCmd)

	// Add to root command
	rootCmd.AddCommand(authCmd)
}
