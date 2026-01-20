package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iamcredentials/v1"
	"google.golang.org/api/idtoken"
	"google.golang.org/api/option"
)

var (
	authCredentials           string
	authAudience              string
	authImpersonateServiceAccount string
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

  # Print identity token with impersonation (user credentials)
  cio auth print-identity-token \
    -a https://example-service.run.app \
    --impersonate-service-account=my-sa@project.iam.gserviceaccount.com

  # Print identity token using service account file
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

For user credentials (from 'gcloud auth application-default login'), you must
specify a service account to impersonate using --impersonate-service-account.

Examples:
  # Using service account file
  cio auth print-identity-token \
    -a https://my-service.run.app \
    -c /path/to/service-account.json

  # Using user credentials with impersonation
  cio auth print-identity-token \
    -a https://my-service.run.app \
    --impersonate-service-account=my-sa@project.iam.gserviceaccount.com

  # Use in curl command
  curl -H "Authorization: Bearer $(cio auth print-identity-token -a https://my-service.run.app)" \
    https://my-service-abc123.run.app`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if authAudience == "" {
			return fmt.Errorf("audience is required (use -a or --audience flag)")
		}

		ctx := context.Background()

		// Case 1: Service account credentials file provided
		if authCredentials != "" {
			ts, err := idtoken.NewTokenSource(ctx, authAudience, option.WithCredentialsFile(authCredentials))
			if err != nil {
				return fmt.Errorf("failed to create token source: %w", err)
			}

			token, err := ts.Token()
			if err != nil {
				return fmt.Errorf("failed to get identity token: %w", err)
			}

			fmt.Println(token.AccessToken)
			return nil
		}

		// Case 2: User credentials with impersonation
		if authImpersonateServiceAccount != "" {
			token, err := generateIdentityTokenWithImpersonation(ctx, authImpersonateServiceAccount, authAudience)
			if err != nil {
				return err
			}

			fmt.Println(token)
			return nil
		}

		// Case 3: Try standard ADC (works for some credential types)
		ts, err := idtoken.NewTokenSource(ctx, authAudience)
		if err != nil {
			// Check if this is the unsupported credentials type error
			errMsg := err.Error()
			if errMsg == "idtoken: unsupported credentials type" {
				return fmt.Errorf(`identity tokens require service account credentials or impersonation

User credentials from 'gcloud auth application-default login' cannot generate
identity tokens directly. You must use one of these options:

  1. Use a service account JSON file:
     cio auth print-identity-token -a %s -c /path/to/service-account.json

  2. Use service account impersonation (recommended):
     cio auth print-identity-token -a %s \
       --impersonate-service-account=my-sa@project.iam.gserviceaccount.com

  3. Use gcloud directly:
     gcloud auth print-identity-token --audiences=%s

Note: For option 2, you need the 'Service Account Token Creator' role on the
service account you want to impersonate.`, authAudience, authAudience, authAudience)
			}
			return fmt.Errorf("failed to create token source: %w", err)
		}

		token, err := ts.Token()
		if err != nil {
			return fmt.Errorf("failed to get identity token: %w", err)
		}

		fmt.Println(token.AccessToken)
		return nil
	},
}

// generateIdentityTokenWithImpersonation generates an identity token by impersonating a service account.
func generateIdentityTokenWithImpersonation(ctx context.Context, serviceAccount, audience string) (string, error) {
	// Get credentials for IAM API (uses ADC)
	creds, err := google.FindDefaultCredentials(ctx, iamcredentials.CloudPlatformScope)
	if err != nil {
		return "", fmt.Errorf("failed to get credentials: %w", err)
	}

	// Create IAM Credentials service
	iamService, err := iamcredentials.NewService(ctx, option.WithTokenSource(creds.TokenSource))
	if err != nil {
		return "", fmt.Errorf("failed to create IAM service: %w", err)
	}

	// Generate identity token
	name := fmt.Sprintf("projects/-/serviceAccounts/%s", serviceAccount)
	req := &iamcredentials.GenerateIdTokenRequest{
		Audience:     audience,
		IncludeEmail: true,
	}

	resp, err := iamService.Projects.ServiceAccounts.GenerateIdToken(name, req).Context(ctx).Do()
	if err != nil {
		return "", fmt.Errorf("failed to generate identity token: %w\n\nMake sure you have the 'Service Account Token Creator' role (roles/iam.serviceAccountTokenCreator)\non service account: %s", err, serviceAccount)
	}

	return resp.Token, nil
}

func init() {
	// Add auth command flags
	authCmd.PersistentFlags().StringVarP(&authCredentials, "credentials", "c", "", "Path to service account JSON file")

	// Add print-identity-token flags
	printIdentityTokenCmd.Flags().StringVarP(&authAudience, "audience", "a", "", "Target audience URL (e.g., https://example-service.run.app)")
	printIdentityTokenCmd.Flags().StringVar(&authImpersonateServiceAccount, "impersonate-service-account", "", "Service account to impersonate (e.g., my-sa@project.iam.gserviceaccount.com)")

	// Add subcommands
	authCmd.AddCommand(printAccessTokenCmd)
	authCmd.AddCommand(printIdentityTokenCmd)

	// Add to root command
	rootCmd.AddCommand(authCmd)
}
