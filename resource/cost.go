package resource

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	gcpbq "cloud.google.com/go/bigquery"
	"github.com/thieso2/cio/bigquery"
	"github.com/thieso2/cio/resolver"
)

const TypeCost Type = "cost"

// CostInfo holds cost data for a single row (service, project, or date).
type CostInfo struct {
	Label    string  `json:"label"`
	Project  string  `json:"project,omitempty"`
	Date     string  `json:"date,omitempty"`
	Service  string  `json:"service,omitempty"`
	Cost     float64 `json:"gross_cost"`
	Credits  float64 `json:"credits"`
	NetCost  float64 `json:"net_cost"`
	Currency string  `json:"currency"`
}

func (c *CostInfo) FormatShortW(w int) string {
	return fmt.Sprintf("%-*s %10.2f %s", w, c.Label, c.NetCost, c.Currency)
}

func (c *CostInfo) FormatShort() string {
	return c.FormatShortW(45)
}

func (c *CostInfo) FormatLongW(w int) string {
	return fmt.Sprintf("%-*s %12.2f %12.2f %12.2f %s",
		w, c.Label, c.Cost, c.Credits, c.NetCost, c.Currency)
}

func (c *CostInfo) FormatLong() string {
	return c.FormatLongW(45)
}

func CostShortHeaderW(w int) string {
	return fmt.Sprintf("%-*s %10s", w, "NAME", "COST")
}

func CostShortHeader() string {
	return CostShortHeaderW(45)
}

func CostLongHeaderW(w int) string {
	return fmt.Sprintf("%-*s %12s %12s %12s", w, "NAME", "GROSS", "CREDITS", "NET COST")
}

func CostLongHeader() string {
	return CostLongHeaderW(45)
}

// CostLabelWidth computes the appropriate label column width from the data.
func CostLabelWidth(resources []*ResourceInfo) int {
	w := 20 // minimum
	for _, info := range resources {
		if c, ok := info.Metadata.(*CostInfo); ok {
			if len(c.Label) > w {
				w = len(c.Label)
			}
		}
	}
	return w + 2 // padding
}

// costView determines how to aggregate costs
type costView int

const (
	costByService costView = iota
	costByProject
	costByDay
	costByDayService // daily breakdown by service
	costByDayOne     // daily for a specific service
)

// parsedCostPath holds the parsed components of a cost:// path
type parsedCostPath struct {
	project       string   // optional project filter
	view          costView // aggregation mode
	month         string   // YYYYMM format, empty for current month
	serviceFilter string   // specific service name for costByDayOne
}

func parseCostPath(path string) parsedCostPath {
	after := strings.TrimPrefix(path, "cost://")
	p := parsedCostPath{view: costByService}

	parts := strings.Split(after, "/")
	// Filter out empty parts
	var nonEmpty []string
	for _, s := range parts {
		if s != "" {
			nonEmpty = append(nonEmpty, s)
		}
	}

	if len(nonEmpty) == 0 {
		return p
	}

	// Check for special views (parse from the end)
	// Patterns: daily, daily/services, daily/ServiceName, projects
	last := nonEmpty[len(nonEmpty)-1]
	switch {
	case last == "projects":
		p.view = costByProject
		nonEmpty = nonEmpty[:len(nonEmpty)-1]
	case last == "services" && len(nonEmpty) >= 2 && nonEmpty[len(nonEmpty)-2] == "daily":
		// daily/services
		p.view = costByDayService
		nonEmpty = nonEmpty[:len(nonEmpty)-2]
	case last == "daily":
		p.view = costByDay
		nonEmpty = nonEmpty[:len(nonEmpty)-1]
	case len(nonEmpty) >= 2 && nonEmpty[len(nonEmpty)-2] == "daily":
		// daily/<ServiceName>
		p.view = costByDayOne
		p.serviceFilter = last
		nonEmpty = nonEmpty[:len(nonEmpty)-2]
	}

	if len(nonEmpty) > 0 {
		p.project = nonEmpty[0]
	}

	return p
}

// CostResource implements Resource for billing cost data via BigQuery
type CostResource struct {
	formatter    PathFormatter
	billingTable string
	labelWidth   int      // dynamically set after listing
	lastView     costView // set after List() for header formatting
	subWidths    [3]int   // project, date, service widths for dayService view
}

func CreateCostResource(formatter PathFormatter, billingTable string) *CostResource {
	return &CostResource{formatter: formatter, billingTable: billingTable}
}

func (r *CostResource) Type() Type     { return TypeCost }
func (r *CostResource) SupportsInfo() bool { return false }

func (r *CostResource) List(ctx context.Context, path string, opts *ListOptions) ([]*ResourceInfo, error) {
	if r.billingTable == "" {
		return nil, fmt.Errorf("billing table not configured: add billing.table to your config file\n\nExample:\n  billing:\n    table: my-project.billing_dataset.gcp_billing_export_v1_XXXXXX")
	}

	parsed := parseCostPath(path)

	// Month priority: opts > parsed > current month
	month := ""
	if opts != nil && opts.Month != "" {
		month = opts.Month
	} else if parsed.month != "" {
		month = parsed.month
	} else {
		month = time.Now().Format("200601")
	}

	sql := r.buildQuery(parsed, month)

	// Extract project ID from billing table name (first component)
	bqProject := strings.SplitN(r.billingTable, ".", 2)[0]

	result, err := bigquery.ExecuteQuery(ctx, bqProject, sql, 0)
	if err != nil {
		return nil, fmt.Errorf("billing query failed: %w", err)
	}

	r.lastView = parsed.view

	// For costByDayService, rows have 7 columns (proj, dt, svc, gross, credits, net, currency).
	// We need to compute max widths for each sub-column, then compose fixed-width labels.
	if parsed.view == costByDayService {
		return r.buildDayServiceResults(result.Rows)
	}

	var resources []*ResourceInfo
	for _, row := range result.Rows {
		info := r.rowToCostInfo(row)
		if info == nil {
			continue
		}
		resources = append(resources, &ResourceInfo{
			Name:     info.Label,
			Path:     "cost://" + info.Label,
			Type:     "cost",
			Metadata: info,
		})
	}

	r.labelWidth = CostLabelWidth(resources)

	return resources, nil
}

// buildDayServiceResults formats 7-column rows (proj, dt, svc, gross, credits, net, currency)
// with fixed-width sub-columns for aligned output.
func (r *CostResource) buildDayServiceResults(rows [][]gcpbq.Value) ([]*ResourceInfo, error) {
	type rawRow struct {
		proj, dt, svc    string
		gross, cred, net float64
		currency         string
	}

	var parsed []rawRow
	var maxProj, maxSvc int
	for _, row := range rows {
		if len(row) < 7 {
			continue
		}
		proj, _ := row[0].(string)
		dt, _ := row[1].(string)
		svc, _ := row[2].(string)
		if len(proj) > maxProj {
			maxProj = len(proj)
		}
		if len(svc) > maxSvc {
			maxSvc = len(svc)
		}
		parsed = append(parsed, rawRow{
			proj: proj, dt: dt, svc: svc,
			gross: toFloat64(row[3]), cred: toFloat64(row[4]),
			net: toFloat64(row[5]), currency: toString(row[6]),
		})
	}

	r.subWidths = [3]int{maxProj, 10, maxSvc} // date is always 10 chars (YYYY-MM-DD)

	var resources []*ResourceInfo
	for _, rr := range parsed {
		label := fmt.Sprintf("%-*s  %s  %-*s", maxProj, rr.proj, rr.dt, maxSvc, rr.svc)
		info := &CostInfo{
			Label:    label,
			Project:  rr.proj,
			Date:     rr.dt,
			Service:  rr.svc,
			Cost:     rr.gross,
			Credits:  rr.cred,
			NetCost:  rr.net,
			Currency: rr.currency,
		}
		resources = append(resources, &ResourceInfo{
			Name:     label,
			Path:     "cost://" + label,
			Type:     "cost",
			Metadata: info,
		})
	}

	r.labelWidth = CostLabelWidth(resources)
	return resources, nil
}

func toString(v interface{}) string {
	s, _ := v.(string)
	return s
}

func (r *CostResource) buildQuery(parsed parsedCostPath, month string) string {
	table := fmt.Sprintf("`%s`", r.billingTable)
	creditExpr := `IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)`
	whereClause := fmt.Sprintf("WHERE invoice.month = '%s'", month)

	if parsed.project != "" {
		if resolver.HasWildcard(parsed.project) {
			// Convert glob to SQL LIKE: * → %, ? → _
			like := strings.ReplaceAll(parsed.project, "*", "%")
			like = strings.ReplaceAll(like, "?", "_")
			whereClause += fmt.Sprintf(" AND project.id LIKE '%s'", like)
		} else {
			whereClause += fmt.Sprintf(" AND project.id = '%s'", parsed.project)
		}
	}

	switch parsed.view {
	case costByProject:
		return fmt.Sprintf(
			`SELECT project.id, SUM(cost) AS gross, SUM(%s) AS credits, `+
				`SUM(cost) + SUM(%s) AS net, currency `+
				`FROM %s %s `+
				`GROUP BY project.id, currency ORDER BY net DESC`,
			creditExpr, creditExpr, table, whereClause)
	case costByDay:
		return fmt.Sprintf(
			`SELECT CAST(DATE(usage_start_time) AS STRING) AS dt, SUM(cost) AS gross, SUM(%s) AS credits, `+
				`SUM(cost) + SUM(%s) AS net, currency `+
				`FROM %s %s `+
				`GROUP BY dt, currency ORDER BY dt`,
			creditExpr, creditExpr, table, whereClause)
	case costByDayService:
		return fmt.Sprintf(
			`SELECT proj, dt, svc, gross, credits, net, currency FROM (`+
				`SELECT project.id AS proj, CAST(DATE(usage_start_time) AS STRING) AS dt, service.description AS svc, `+
				`SUM(cost) AS gross, SUM(%s) AS credits, SUM(cost) + SUM(%s) AS net, currency `+
				`FROM %s %s `+
				`GROUP BY proj, dt, svc, currency) ORDER BY proj, dt, net DESC`,
			creditExpr, creditExpr, table, whereClause)
	case costByDayOne:
		whereClause += fmt.Sprintf(" AND service.description = '%s'", parsed.serviceFilter)
		return fmt.Sprintf(
			`SELECT CAST(DATE(usage_start_time) AS STRING) AS dt, SUM(cost) AS gross, SUM(%s) AS credits, `+
				`SUM(cost) + SUM(%s) AS net, currency `+
				`FROM %s %s `+
				`GROUP BY dt, currency ORDER BY dt`,
			creditExpr, creditExpr, table, whereClause)
	default: // costByService
		return fmt.Sprintf(
			`SELECT service.description, SUM(cost) AS gross, SUM(%s) AS credits, `+
				`SUM(cost) + SUM(%s) AS net, currency `+
				`FROM %s %s `+
				`GROUP BY service.description, currency ORDER BY net DESC`,
			creditExpr, creditExpr, table, whereClause)
	}
}

func (r *CostResource) rowToCostInfo(row []gcpbq.Value) *CostInfo {
	if len(row) < 5 {
		return nil
	}

	label, _ := row[0].(string)
	gross := toFloat64(row[1])
	credits := toFloat64(row[2])
	net := toFloat64(row[3])
	currency, _ := row[4].(string)

	return &CostInfo{
		Label:    label,
		Cost:     gross,
		Credits:  credits,
		NetCost:  net,
		Currency: currency,
	}
}

func toFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int64:
		return float64(n)
	case int:
		return float64(n)
	default:
		return 0
	}
}

func (r *CostResource) Remove(_ context.Context, _ string, _ *RemoveOptions) error {
	return fmt.Errorf("removing cost data is not supported")
}

func (r *CostResource) Info(_ context.Context, _ string) (*ResourceInfo, error) {
	return nil, fmt.Errorf("info is not supported for cost data")
}

func (r *CostResource) ParsePath(path string) (*PathComponents, error) {
	return &PathComponents{ResourceType: TypeCost}, nil
}

func (r *CostResource) FormatShort(info *ResourceInfo, aliasPath string) string {
	if c, ok := info.Metadata.(*CostInfo); ok {
		return c.FormatShortW(r.labelWidth)
	}
	return info.Name
}

func (r *CostResource) FormatLong(info *ResourceInfo, aliasPath string) string {
	if c, ok := info.Metadata.(*CostInfo); ok {
		return c.FormatLongW(r.labelWidth)
	}
	return info.Name
}

func (r *CostResource) FormatDetailed(info *ResourceInfo, aliasPath string) string {
	return r.FormatLong(info, aliasPath)
}

func (r *CostResource) FormatHeader() string {
	if r.lastView == costByDayService && r.subWidths[0] > 0 {
		label := fmt.Sprintf("%-*s  %-*s  %-*s",
			r.subWidths[0], "PROJECT", r.subWidths[1], "DATE", r.subWidths[2], "SERVICE")
		w := len(label) + 2
		return fmt.Sprintf("%-*s %10s", w, label, "COST")
	}
	return CostShortHeaderW(r.labelWidth)
}

func (r *CostResource) FormatLongHeader() string {
	if r.lastView == costByDayService && r.subWidths[0] > 0 {
		label := fmt.Sprintf("%-*s  %-*s  %-*s",
			r.subWidths[0], "PROJECT", r.subWidths[1], "DATE", r.subWidths[2], "SERVICE")
		w := len(label) + 2
		return fmt.Sprintf("%-*s %12s %12s %12s", w, label, "GROSS", "CREDITS", "NET COST")
	}
	return CostLongHeaderW(r.labelWidth)
}

// PrintCostTotal prints a total row summarizing all cost resources.
func PrintCostTotal(resources []*ResourceInfo, longFormat bool) {
	w := CostLabelWidth(resources)
	var totalGross, totalCredits, totalNet float64
	var currency string
	for _, info := range resources {
		if c, ok := info.Metadata.(*CostInfo); ok {
			totalGross += c.Cost
			totalCredits += c.Credits
			totalNet += c.NetCost
			if currency == "" {
				currency = c.Currency
			}
		}
	}

	// Match separator width to the formatted line width
	sepWidth := w + 40
	if longFormat {
		sepWidth = w + 42
	}
	fmt.Println(strings.Repeat("-", sepWidth))
	total := &CostInfo{Label: "TOTAL", Cost: totalGross, Credits: totalCredits, NetCost: totalNet, Currency: currency}
	if longFormat {
		fmt.Println(total.FormatLongW(w))
	} else {
		fmt.Println(total.FormatShortW(w))
	}
}

// SortCostBy sorts cost resources by the given field descending.
// Valid fields: "cost" or "net" (net cost), "gross" (gross cost), "credits".
func SortCostBy(resources []*ResourceInfo, field string) {
	sort.Slice(resources, func(i, j int) bool {
		ci, _ := resources[i].Metadata.(*CostInfo)
		cj, _ := resources[j].Metadata.(*CostInfo)
		if ci == nil || cj == nil {
			return false
		}
		switch field {
		case "gross":
			return math.Abs(ci.Cost) > math.Abs(cj.Cost)
		case "credits":
			return math.Abs(ci.Credits) > math.Abs(cj.Credits)
		default: // "cost", "net"
			return math.Abs(ci.NetCost) > math.Abs(cj.NetCost)
		}
	})
}
