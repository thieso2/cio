package cli

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/resource"
)

var (
	lsLongFormat    bool
	lsHumanReadable bool
	lsRecursive     bool
	lsMaxResults    int
	lsNoMap         bool
	lsRaw           bool
	lsSortBySize    bool
	lsSortByTime    bool
	lsActiveOnly    bool
	lsAll           bool
	lsMonth         string
	lsSort          string
)

var lsCmd = &cobra.Command{
	Use:   "ls <path>",
	Short: "List GCS buckets/objects, BigQuery datasets/tables, Dataflow jobs, VMs, or Pub/Sub resources",
	Long: `List GCS buckets, objects, BigQuery datasets/tables, or Dataflow jobs using an alias or full path.

The path can be either:
  - An alias (with : prefix): ':am', ':am/2024/', ':am/2024/01/data.txt'
  - A full GCS path: 'gs://bucket-name/', 'gs://bucket-name/prefix/'
  - List all buckets: 'gs://project-id:' (note the colon at the end)
  - A full BigQuery path: 'bq://project-id', 'bq://project-id.dataset'
  - List all datasets: 'bq://' (uses default project from config)
  - Wildcard pattern: ':am/logs/*.log', ':am/data/2024-*.csv'
  - Dataflow jobs: 'dataflow://' (all jobs), 'dataflow://pattern*', --active for active only
  - VM zones: 'vm://' (list zones with instance counts)
  - VM instances: 'vm://zone', 'vm://zone/pattern*', 'vm://*/pattern*' (all zones)

Examples (GCS):
  # List buckets in a project
  cio ls 'gs://my-project-id:'

  # List objects in bucket
  cio ls :am
  cio ls -l :am/2024/
  cio ls ':am/logs/*.log'

Examples (BigQuery):
  # List datasets in default project
  cio ls bq://

  # List datasets in specific project
  cio ls bq://my-project-id

  # List tables in dataset
  cio ls :mydata
  cio ls ':mydata.events_*'

Examples (Cloud Run Jobs):
  # List executions (active only by default, newest first)
  cio ls -l jobs://my-job/

  # List all executions (include completed/failed)
  cio ls -la jobs://my-job/

Examples (Dataflow):
  # List all Dataflow jobs (active + terminated)
  cio ls dataflow://

  # List only active jobs
  cio ls --active dataflow://

  # List jobs matching a pattern
  cio ls 'dataflow://my-pipeline*'

  # Long format with state, type, created time
  cio ls -l dataflow://

Examples (VM):
  # List zones with instance counts
  cio ls vm://

  # List instances in a specific zone
  cio ls vm://europe-west3-a

  # List instances matching a pattern in a zone
  cio ls 'vm://europe-west3-a/web-*'

  # List instances matching a pattern across all zones
  cio ls 'vm://*/iomp*'

  # Long format with status, machine type, IP
  cio ls -l vm://europe-west3-a

Examples (Pub/Sub):
  # List all topics and subscriptions
  cio ls pubsub://

  # List topics only
  cio ls pubsub://topics

  # List subscriptions only
  cio ls pubsub://subs

  # Long format with metrics
  cio ls -l pubsub://subs

  # Wildcard patterns
  cio ls 'pubsub://topics/events-*'
  cio ls 'pubsub://subs/staging-*'`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		path := args[0]

		// Check for discover mode: scheme:/project-pattern/rest
		if projectPattern, scheme, rest, ok := parseDiscoverPath(path); ok {
			if scheme == "cost" {
				// cost:// handles project filtering internally via SQL — rewrite to cost:// path
				path = "cost://" + projectPattern
				if rest != "" {
					path += "/" + rest
				}
			} else {
				return runDiscoverMode(cmd, scheme, projectPattern, rest)
			}
		}

		// Resolve the input and build its resource handler (shared prelude).
		res, r, fullPath, inputWasAlias, err := resolveToResource(path)
		if err != nil {
			return err
		}

		if verbose {
			fmt.Fprintf(os.Stderr, "Listing: %s\n", fullPath)
		}

		ctx := context.Background()

		// List resources
		// Convert --month flag to YYYYMM format if provided as YYYY-MM
		month := lsMonth
		if len(month) == 7 && month[4] == '-' {
			month = month[:4] + month[5:]
		}

		options := &resource.ListOptions{
			Recursive:     lsRecursive,
			LongFormat:    lsLongFormat,
			HumanReadable: lsHumanReadable,
			MaxResults:    lsMaxResults,
			ProjectID:     cfg.Defaults.ProjectID,
			Region:        cfg.Defaults.Region,
			ActiveOnly:    lsActiveOnly,
			AllStatuses:   lsAll,
			Month:         month,
		}

		resources, err := res.List(ctx, fullPath, options)
		if err != nil {
			return fmt.Errorf("failed to list resources: %w", err)
		}

		// Sort resources — Cloud Run and Dataflow default to newest first
		sortByTime := lsSortByTime
		if !lsSortBySize && !lsSortByTime {
			if resolver.IsCloudRunPath(fullPath) || resolver.IsDataflowPath(fullPath) {
				sortByTime = true
			}
		}
		sortResources(resources, lsSortBySize, sortByTime)

		// Cost-specific sorting: --sort=cost|net|gross|credits
		if resolver.IsCostPath(fullPath) && lsSort != "" {
			resource.SortCostBy(resources, lsSort)
		}

		// Handle empty results
		if len(resources) == 0 {
			if verbose {
				fmt.Fprintf(os.Stderr, "No resources found\n")
			}
			return nil
		}

		// Determine whether to reverse-map output
		// Only reverse-map if: input was an alias AND --no-map flag is not set
		shouldReverseMap := inputWasAlias && !lsNoMap

		// JSON mode: output all resources as JSON array
		if outputJSON {
			return printResourcesJSON(resources)
		}

		// Raw mode: output paths without protocol prefix
		if lsRaw {
			for _, info := range resources {
				rawPath := extractRawPath(info.Path)
				fmt.Println(rawPath)
			}
			return nil
		}

		// Pub/Sub "both" mode: print topics and subs in separate sections
		if resolver.IsPubSubPath(fullPath) && hasMixedTypes(resources) {
			printPubSubSections(resources, res, r, shouldReverseMap, lsLongFormat)
			return nil
		}

		// Cost keeps its own adaptive formatting (right-aligned money columns,
		// dynamic label widths, and a trailing total) — it prints directly
		// rather than through the tab-aligned renderer.
		if resolver.IsCostPath(fullPath) {
			if cr, ok := res.(*resource.CostResource); ok {
				if p := cr.Period(); p != "" {
					fmt.Fprintf(os.Stderr, "Period: %s\n", p)
				}
				if lsLongFormat {
					fmt.Println(cr.FormatLongHeader())
				} else {
					fmt.Println(cr.FormatHeader())
				}
			}
			for _, info := range resources {
				if lsLongFormat {
					fmt.Println(res.FormatLong(info, info.Path))
				} else {
					fmt.Println(res.FormatShort(info, info.Path))
				}
			}
			if len(resources) > 1 {
				resource.PrintCostTotal(resources, lsLongFormat)
			}
			return nil
		}

		// Everything else: build tab-separated rows and let renderTable size
		// each column to its data.
		header := ""
		if lsLongFormat {
			if resolver.IsCloudRunPath(fullPath) || resolver.IsDataflowPath(fullPath) {
				header = resource.FormatLongHeaderDynamic(resources)
			} else {
				header = res.FormatLongHeader()
			}
		}

		rows := make([]string, 0, len(resources))
		for _, info := range resources {
			displayPath := info.Path
			if shouldReverseMap {
				displayPath = r.ReverseResolve(info.Path)
			}
			if lsLongFormat {
				rows = append(rows, res.FormatLong(info, displayPath))
			} else {
				rows = append(rows, res.FormatShort(info, displayPath))
			}
		}
		renderTable(header, rows, "")

		return nil
	},
}

// hasMixedTypes returns true if resources contain both topics and subscriptions.
func hasMixedTypes(resources []*resource.ResourceInfo) bool {
	hasTopic := false
	hasSub := false
	for _, r := range resources {
		if r.Type == "topic" {
			hasTopic = true
		} else if r.Type == "subscription" {
			hasSub = true
		}
		if hasTopic && hasSub {
			return true
		}
	}
	return false
}

// printPubSubSections prints topics and subscriptions in separate sections.
func printPubSubSections(resources []*resource.ResourceInfo, res resource.Resource, r *resolver.Resolver, shouldReverseMap, longFormat bool) {
	var topics, subs []*resource.ResourceInfo
	for _, info := range resources {
		if info.Type == "topic" {
			topics = append(topics, info)
		} else {
			subs = append(subs, info)
		}
	}

	section := func(title string, infos []*resource.ResourceInfo) {
		fmt.Printf("%s (%d):\n", title, len(infos))
		rows := make([]string, 0, len(infos))
		for _, info := range infos {
			displayPath := info.Path
			if shouldReverseMap {
				displayPath = r.ReverseResolve(info.Path)
			}
			if longFormat {
				rows = append(rows, res.FormatLong(info, displayPath))
			} else {
				rows = append(rows, res.FormatShort(info, displayPath))
			}
		}
		renderTable("", rows, "  ")
	}

	if len(topics) > 0 {
		section("Topics", topics)
	}
	if len(topics) > 0 && len(subs) > 0 {
		fmt.Println()
	}
	if len(subs) > 0 {
		section("Subscriptions", subs)
	}
}

// extractRawPath removes the protocol prefix from a path
// For BigQuery: bq://project.dataset.table -> project.dataset.table
// For GCS: gs://bucket/path/to/object -> bucket/path/to/object
func extractRawPath(path string) string {
	// Remove protocol prefix
	path = strings.TrimPrefix(path, "gs://")
	path = strings.TrimPrefix(path, "bq://")
	return path
}

// sortResources sorts resources based on the specified flags
// Default: sort by name (path)
// -S: sort by size (descending)
// -t: sort by time (newest first)
func sortResources(resources []*resource.ResourceInfo, bySize, byTime bool) {
	if bySize {
		// Sort by size, descending (largest first)
		sort.Slice(resources, func(i, j int) bool {
			if resources[i].Size != resources[j].Size {
				return resources[i].Size > resources[j].Size
			}
			// Secondary sort by name
			return resources[i].Path < resources[j].Path
		})
	} else if byTime {
		// Sort by time, descending (newest first)
		sort.Slice(resources, func(i, j int) bool {
			if !resources[i].Modified.Equal(resources[j].Modified) {
				return resources[i].Modified.After(resources[j].Modified)
			}
			// Secondary sort by name
			return resources[i].Path < resources[j].Path
		})
	} else {
		// Default: sort by name (path), ascending
		sort.Slice(resources, func(i, j int) bool {
			return resources[i].Path < resources[j].Path
		})
	}
}

// runDiscoverMode lists resources across multiple projects matching a pattern.
func runDiscoverMode(cmd *cobra.Command, scheme, projectPattern, rest string) error {
	ctx := context.Background()

	r := resolver.Create(cfg)
	factory := resource.CreateFactory(r.ReverseResolve)

	// JSON mode: collect all resources across projects, output as single array.
	// Text mode: collect tab-separated rows (and the header once) so the whole
	// cross-project listing aligns as one table, rendered after the walk.
	var allResources []*resource.ResourceInfo
	var rows []string
	header := ""
	headerCaptured := false

	err := forEachDiscoveredProject(ctx, scheme, projectPattern, rest, func(projectID, resourcePath string) error {
		if verbose {
			fmt.Fprintf(os.Stderr, "Listing %s in project %s\n", resourcePath, projectID)
		}

		res, err := factory.Create(resourcePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: %s: %v\n", projectID, err)
			return nil
		}

		options := &resource.ListOptions{
			Recursive:     lsRecursive,
			LongFormat:    lsLongFormat,
			HumanReadable: lsHumanReadable,
			MaxResults:    lsMaxResults,
			ProjectID:     projectID,
			Region:        cfg.Defaults.Region,
			ActiveOnly:    lsActiveOnly,
			AllStatuses:   lsAll,
		}

		resources, err := res.List(ctx, resourcePath, options)
		if err != nil {
			if verbose {
				fmt.Fprintf(os.Stderr, "Warning: %s: %v\n", projectID, err)
			}
			return nil
		}

		if len(resources) == 0 {
			return nil
		}

		// Sort
		sortByTime := lsSortByTime
		if !lsSortBySize && !lsSortByTime {
			if resolver.IsCloudRunPath(resourcePath) || resolver.IsDataflowPath(resourcePath) {
				sortByTime = true
			}
		}
		sortResources(resources, lsSortBySize, sortByTime)

		// Capture the header once (first project that returns rows).
		if lsLongFormat && !headerCaptured {
			if resolver.IsCloudRunPath(resourcePath) || resolver.IsDataflowPath(resourcePath) {
				header = resource.FormatLongHeaderDynamic(resources)
			} else {
				header = res.FormatLongHeader()
			}
			headerCaptured = true
		}

		// Prefix project name to each resource
		for _, info := range resources {
			prefixResourceName(info, scheme, projectID)
		}

		if outputJSON {
			allResources = append(allResources, resources...)
			return nil
		}

		// Collect rows with project prefix for one aligned render.
		for _, info := range resources {
			if lsLongFormat {
				rows = append(rows, res.FormatLong(info, info.Path))
			} else {
				rows = append(rows, res.FormatShort(info, info.Path))
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	if !outputJSON {
		renderTable(header, rows, "")
	}

	if outputJSON {
		return printResourcesJSON(allResources)
	}

	return nil
}

func init() {
	// Add flags
	lsCmd.Flags().BoolVarP(&lsLongFormat, "long", "l", false, "use long listing format (timestamp, size, path)")
	lsCmd.Flags().BoolVar(&lsHumanReadable, "human-readable", false, "print sizes in human-readable format (e.g., 1.2 MB)")
	lsCmd.Flags().BoolVarP(&lsRecursive, "recursive", "r", false, "list all objects recursively")
	lsCmd.Flags().BoolVarP(&lsRecursive, "Recursive", "R", false, "list all objects recursively (alias for -r)")
	lsCmd.Flags().IntVar(&lsMaxResults, "max-results", 0, "maximum number of results (0 = no limit)")
	lsCmd.Flags().BoolVarP(&lsNoMap, "no-map", "n", false, "show full paths without alias mapping")
	lsCmd.Flags().BoolVar(&lsRaw, "raw", false, "output only resource names, one per line (useful for scripting)")
	lsCmd.Flags().BoolVarP(&lsSortBySize, "sort-size", "S", false, "sort by size (largest first)")
	lsCmd.Flags().BoolVarP(&lsSortByTime, "sort-time", "t", false, "sort by modification time (newest first)")
	lsCmd.Flags().BoolVar(&lsActiveOnly, "active", false, "show only active jobs (Dataflow)")
	lsCmd.Flags().BoolVarP(&lsAll, "all", "a", false, "show all statuses (include completed/failed executions)")
	lsCmd.Flags().StringVar(&lsMonth, "month", "", "billing month (YYYY-MM or YYYYMM, default: current month)")
	lsCmd.Flags().StringVar(&lsSort, "sort", "", "sort order for cost output (cost = by cost descending)")

	// Add to root command
	rootCmd.AddCommand(lsCmd)
}
