package cli

import (
	"fmt"
	"os"
	"text/tabwriter"
)

// renderTable prints an optional header followed by rows, aligning their
// tab-separated cells into columns sized to the widest value in each column.
// Every line is prefixed with indent (use "" for none).
//
// This is the single seam through which list output is aligned: row
// formatters emit '\t'-separated cells and stay ignorant of column widths,
// while this renderer measures the data and pads. A row with no tabs is a
// single column and prints unchanged.
func renderTable(header string, rows []string, indent string) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	if header != "" {
		fmt.Fprintln(tw, indent+header)
	}
	for _, row := range rows {
		fmt.Fprintln(tw, indent+row)
	}
	tw.Flush()
}
