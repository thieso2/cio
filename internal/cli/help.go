package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// schemeHelpTopic documents one path scheme: what it addresses, which cio
// commands it supports, and copy-pasteable examples. `cio help <scheme>://`
// looks the topic up by scheme name; `cio help schemes` lists them all.
type schemeHelpTopic struct {
	scheme  string   // canonical prefix shown in listings, e.g. "cost://"
	aliases []string // alternate scheme names that resolve to this topic
	short   string   // one-line summary for the topic list
	text    string   // full help body (paths, commands, examples)
}

var schemeHelpTopics = []schemeHelpTopic{
	{
		scheme: "gs://",
		short:  "Google Cloud Storage objects and buckets",
		text: `gs:// — Google Cloud Storage (objects and buckets)

Paths:
  gs://bucket/path/                objects under a prefix
  gs://project-id:                 all buckets in a project (ls-new)
  :alias/path/                     via 'cio map <alias> gs://bucket/'

Commands:
  ls       list objects            -l, -r, --human-readable, --max-results, --json
  cp       copy local <-> GCS      -r (recursive), -j N (parallel chunks)
  cat      print object(s) to stdout
  du       disk usage of a prefix
  rm       delete objects          -r, -f, wildcards (preview + confirmation)
  mount    FUSE filesystem (experimental)

Wildcards: * and ? match object names — quote them in the shell.

Examples:
  cio map am gs://my-bucket/
  cio ls -l :am/2024/
  cio ls ':am/logs/*.log'
  cio cp file.txt :am/2024/01/
  cio cp -j 4 :am/big.zip /tmp/
  cio cat :am/2024/01/data.txt
  cio du :am/2024/
  cio rm ':am/temp/*.tmp'
  cio ls-new 'gs://my-project-id:'
`,
	},
	{
		scheme: "bq://",
		short:  "BigQuery datasets and tables",
		text: `bq:// — BigQuery (datasets and tables)

Paths:
  bq://project-id                  datasets in a project
  bq://project-id.dataset          tables in a dataset
  bq://project-id.dataset.table    a single table
  :alias.table                     via 'cio map <alias> bq://project-id.dataset'

Commands:
  ls       list datasets/tables    -l (type, size, rows), wildcards, --json
  info     table schema + metadata (nested RECORD fields, location, row count)
  rm       delete tables/datasets  -r (whole dataset), -f, wildcards
  query    interactive SQL shell   (alias resolution, \d <table>, history)

Examples:
  cio map mydata bq://my-project-id.my-dataset
  cio ls :mydata
  cio ls -l ':mydata.events_*'
  cio info :mydata.events
  cio rm ':mydata.temp_*'
  cio rm -r :mydata
  cio query
`,
	},
	{
		scheme: "iam://",
		short:  "IAM service accounts",
		text: `iam:// — IAM service accounts

Paths:
  iam://project-id/service-accounts          all service accounts
  iam://project-id/service-accounts/EMAIL    a single service account

Commands:
  ls       list service accounts   -l (email, display name, disabled), --json
  info     service account details
  mount    browse via FUSE (~/gcs/iam/service-accounts/)

Examples:
  cio ls iam://my-project-id/service-accounts
  cio ls -l iam://my-project-id/service-accounts
  cio info iam://my-project-id/service-accounts/my-sa@my-project-id.iam.gserviceaccount.com
`,
	},
	{
		scheme: "svc://",
		short:  "Cloud Run services",
		text: `svc:// — Cloud Run services

Paths:
  svc://                           all services in the default project
  svc://name                       a single service
  svc:/project-pattern/            discover mode (across matching projects)

Commands:
  ls       list services           -l (status, updated), wildcards, --json
  tail     show/stream logs        -f (follow), -n N, -s SEVERITY

Examples:
  cio ls -l svc://
  cio ls -l 'svc:/iom-*/'
  cio tail svc://my-service
  cio tail -f svc:/my-project/my-service
`,
	},
	{
		scheme: "jobs://",
		short:  "Cloud Run jobs and executions",
		text: `jobs:// — Cloud Run jobs and executions

Paths:
  jobs://                          all jobs in the default project
  jobs://job-name                  a single job
  jobs://job-name/execution        a job execution
  jobs:/project-pattern/...        discover mode (across matching projects)

Commands:
  ls       list jobs               -l (status, active/total executions, updated)
  tail     show/stream logs        -f (follow), -n N, -s SEVERITY
  cancel   cancel running executions   -f (skip confirmation), wildcards
  rm       delete executions

Examples:
  cio ls -l jobs://
  cio tail -f jobs://my-job
  cio tail -f jobs:/my-project/          # all jobs in a specific project
  cio cancel 'jobs://my-job/*'
  cio cancel jobs://my-job/my-job-execution-abc123
  cio cancel 'jobs:/iom-*/sqlmesh*/*'    # across projects
`,
	},
	{
		scheme: "worker://",
		short:  "Cloud Run worker pools",
		text: `worker:// — Cloud Run worker pools

Paths:
  worker://                        all worker pools in the default project
  worker://pool-name               a single worker pool

Commands:
  ls       list worker pools       -l (status, instance count, updated)
  scale    set manual instance count (0 to stop)
  tail     show/stream logs        -f (follow), -n N, -s SEVERITY

Examples:
  cio ls -l worker://
  cio scale worker://iomp-processor 3
  cio scale worker://iomp-processor 0
  cio tail -f worker://iomp-processor
`,
	},
	{
		scheme: "dataflow://",
		short:  "Dataflow jobs",
		text: `dataflow:// — Dataflow jobs

Paths:
  dataflow://                      jobs in the default project
  dataflow://job-id                a single job
  dataflow:/project/job-id         a job in a specific project

Commands:
  ls       list jobs               -l, --active (running only), --json
  tail     show/stream logs        -f, -n N, --log-type (job/worker)

Examples:
  cio ls -l dataflow://
  cio tail dataflow://my-job-id
  cio tail -f dataflow:/my-project/my-job-id
`,
	},
	{
		scheme: "vm://",
		short:  "Compute Engine VM instances",
		text: `vm:// — Compute Engine VM instances

Paths:
  vm://                            zones with instance counts
  vm://zone                        instances in a zone
  vm://zone/instance               a single instance
  vm://*/pattern*                  wildcard across all zones
  vm:/project-pattern/pattern*     discover mode (zone implicit, all zones)
  vm:/project/zone/pattern*        discover mode with explicit zone

Commands:
  ls       list zones/instances    -l (status, machine type, zone, IP, created)
  stop     stop running instances  -f, parallel, skips already-stopped
  rm       stop + delete instances -f, parallel
  tail     Cloud Logging output    -f, -n N, -s SEVERITY, --audit (lifecycle events)
           serial port: append /serial to the instance path

Examples:
  cio ls vm://
  cio ls -l 'vm://*/iomp*'
  cio stop 'vm://*/bastion-ephemeral*'
  cio rm -f vm://europe-west3-a/my-instance
  cio tail vm://europe-west3-a/my-instance
  cio tail -f 'vm://*/*-ingress*'
  cio tail --audit vm://europe-west3-a/my-instance
  cio tail -f vm://europe-west3-a/my-instance/serial
  cio ls -l 'vm:/iom-*/iomp-ingress*'
`,
	},
	{
		scheme: "sql://",
		short:  "Cloud SQL instances and databases",
		text: `sql:// — Cloud SQL instances and databases

Paths:
  sql://                           all instances in the default project
  sql://instance                   a single instance
  sql://instance/databases         databases of an instance
  sql:/project-pattern/            discover mode (across matching projects)

Commands:
  ls       list instances/databases   -l, wildcards, --json
  info     detailed instance info
  stop     stop instances           -f, parallel, skips already-stopped
  start    start instances          -f, parallel
  rm       delete instances         -f

Examples:
  cio ls -l sql://
  cio ls -l 'sql://staging-*'
  cio info sql://my-instance
  cio ls sql://my-instance/databases
  cio stop sql://my-instance
  cio start 'sql://staging-*'
  cio ls -l 'sql:/iom-*/'
`,
	},
	{
		scheme: "lb://",
		short:  "Load balancers (URL maps, forwarding rules, backends)",
		text: `lb:// — Load balancers

Paths:
  lb://                            URL maps (load balancers)
  lb://forwarding-rules            forwarding rules (frontend IPs)
  lb://backends                    backend services
  lb:/project-pattern/             discover mode (across matching projects)

Commands:
  ls       list LB resources       -l, wildcards, --json

Examples:
  cio ls -l lb://
  cio ls -l lb://forwarding-rules
  cio ls 'lb://backends/staging-*'
  cio ls -l 'lb:/iom-*/'
`,
	},
	{
		scheme: "certs://",
		short:  "Certificate Manager certificates and maps",
		text: `certs:// — Certificate Manager

Paths:
  certs://                         certificates
  certs://maps                     certificate maps
  certs://maps/NAME/entries        entries in a certificate map

Commands:
  ls       list certificates/maps/entries   -l, wildcards, --json

Examples:
  cio ls -l certs://
  cio ls -l certs://maps
  cio ls -l certs://maps/my-cert-map/entries
  cio ls 'certs://prod-*'
`,
	},
	{
		scheme: "scheduler://",
		short:  "Cloud Scheduler jobs (regional)",
		text: `scheduler:// — Cloud Scheduler jobs

Jobs are regional: listing uses --region / defaults.region from config.

Paths:
  scheduler://                     all jobs in the default project/region
  scheduler://job-name             a single job
  scheduler:/project-pattern/...   discover mode (across matching projects)

Commands:
  ls       list jobs               -l (state, schedule, next/last run), --active
  info     detailed job status     (schedule, target, retries, last attempt)
  stop     pause a job             aliases: disable, pause; -f; skips paused jobs
  start    resume a job            aliases: enable, resume; -f; skips enabled jobs

Examples:
  cio ls -l scheduler://
  cio ls --active scheduler://
  cio info scheduler://my-job
  cio disable scheduler://my-job
  cio enable 'scheduler://nightly-*'
  cio disable 'scheduler:/iom-data*/'
`,
	},
	{
		scheme:  "projects://",
		aliases: []string{"project"},
		short:   "GCP projects (list, examine, delete)",
		text: `projects:// — GCP projects (project:// is an alias)

Paths:
  projects://                      all accessible projects
  projects://pattern*              projects matching a wildcard
  project://project-id             a single project (for info/rm)

Commands:
  ls       list projects           -l, wildcards, --json
  info     project details         (number, state, parent, labels, timestamps)
  rm       delete a project        -f; soft delete (~30-day recovery window)

Examples:
  cio ls -l projects://
  cio ls 'projects://iom-*'
  cio info project://my-project-id
  cio rm project://my-project-id
  cio rm 'projects://staging-*'
`,
	},
	{
		scheme: "pubsub://",
		short:  "Pub/Sub topics and subscriptions",
		text: `pubsub:// — Pub/Sub topics and subscriptions

Paths:
  pubsub://topics                  all topics
  pubsub://topics/NAME             a single topic
  pubsub://subs                    all subscriptions
  pubsub://subs/NAME               a single subscription

Commands:
  ls       list topics/subscriptions   -l, wildcards, --json
  info     topic/subscription details
  rm       delete topics/subscriptions -f, wildcards
  tail     subscription metrics        -f streams a snapshot every 30s

Examples:
  cio ls pubsub://topics
  cio ls -l 'pubsub://subs/my-app-*'
  cio info pubsub://subs/my-sub
  cio tail -f pubsub://subs/my-sub
  cio rm 'pubsub://topics/staging-*'
`,
	},
	{
		scheme: "cost://",
		short:  "Billing costs from the BigQuery billing export",
		text: `cost:// — Billing / cost data (BigQuery billing export)

Requires the billing export table in ~/.config/cio/config.yaml:
  billing:
    table: project-id.dataset.gcp_billing_export_v1_XXXXXX_YYYYYY_ZZZZZZ

Paths:
  cost://                          cost by service (current month)
  cost://project-id                cost for one project (wildcards ok)
  cost://projects                  cost by project
  cost://daily                     daily cost trend
  cost://PROJECT/daily             daily trend for matching projects
  cost://daily/SERVICE             daily trend for one service
  cost:/PROJECT/daily/services     daily breakdown by service

Commands:
  ls       query costs             -l (gross/credits/net), --json
           --month YYYY-MM         historical month (default: current)
           --sort=cost|gross|credits

Examples:
  cio ls cost://
  cio ls -l cost://iom-pro*
  cio ls cost://projects
  cio ls -l cost://iom-*/daily
  cio ls cost://daily/BigQuery
  cio ls cost:// --month 2026-02
  cio ls -l --sort=cost cost:/iom-pro*/daily/services
  cio ls --json cost://
`,
	},
}

// schemeName extracts the bare scheme name from a user-typed help topic:
// "cost://" → "cost", "jobs:/proj/x" → "jobs", "vm" → "vm".
func schemeName(arg string) string {
	name := arg
	if i := strings.Index(name, ":"); i >= 0 {
		name = name[:i]
	}
	return strings.ToLower(strings.TrimSpace(name))
}

// findSchemeHelp returns the help topic for a scheme argument, or nil if the
// argument doesn't name a known scheme.
func findSchemeHelp(arg string) *schemeHelpTopic {
	name := schemeName(arg)
	if name == "" {
		return nil
	}
	for i := range schemeHelpTopics {
		t := &schemeHelpTopics[i]
		if strings.TrimSuffix(t.scheme, "://") == name {
			return t
		}
		for _, a := range t.aliases {
			if a == name {
				return t
			}
		}
	}
	return nil
}

// printSchemeList prints the one-line index of all scheme help topics.
func printSchemeList() {
	fmt.Println("Resource schemes (run 'cio help <scheme>://' for commands and examples):")
	fmt.Println()
	for _, t := range schemeHelpTopics {
		fmt.Printf("  %-14s %s\n", t.scheme, t.short)
	}
}

var helpCmd = &cobra.Command{
	Use:   "help [command | scheme:// | schemes]",
	Short: "Help about any command or resource scheme",
	Long: `Help provides help for any command or resource scheme.

Run 'cio help schemes' to list all resource schemes, or
'cio help <scheme>://' (e.g. 'cio help cost://') for the commands,
flags, and examples a resource type supports.`,
	Run: func(c *cobra.Command, args []string) {
		if len(args) > 0 {
			if args[0] == "schemes" {
				printSchemeList()
				return
			}
			if t := findSchemeHelp(args[0]); t != nil {
				fmt.Print(t.text)
				return
			}
		}
		cmd, _, e := rootCmd.Find(args)
		if cmd == nil || e != nil {
			rootCmd.Printf("Unknown help topic %#q\n\n", args)
			printSchemeList()
			return
		}
		cmd.InitDefaultHelpFlag()
		_ = cmd.Help()
	},
}

func init() {
	rootCmd.SetHelpCommand(helpCmd)
}
