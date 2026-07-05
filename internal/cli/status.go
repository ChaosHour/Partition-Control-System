package cli

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/ChaosHour/Partition-Control-System/internal/db"
	"github.com/ChaosHour/Partition-Control-System/internal/pcs"
)

// cmdStatus shows the registered tables with their partition footprint,
// followed by recent pcs_log entries.
func cmdStatus(args []string) int {
	fs := flag.NewFlagSet("pcs status", flag.ExitOnError)
	var conn db.ConnFlags
	conn.Register(fs)
	schema := fs.String("schema", pcs.DefaultSchema, "database holding PCS metadata")
	logLines := fs.Int("log", 10, "number of recent pcs_log entries to show (0 = none)")
	fs.Parse(args)

	ctx := context.Background()
	d, err := conn.Connect(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pcs status: %v\n", err)
		return 1
	}
	defer d.Close()

	store := &pcs.Store{DB: d, Schema: *schema}
	rows, err := store.ConfigRows(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pcs status: %v\n", err)
		return 1
	}

	fmt.Printf("server %s, metadata schema %s\n\n", d.Version.Raw, *schema)
	if len(rows) == 0 {
		fmt.Println("no tables registered in pcs_config")
	} else {
		w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
		fmt.Fprintln(w, "TABLE\tCOLUMN\tSTATE\tKEEP\tHOUR\tPARTITIONS\tOLDEST\tNEWEST")
		for _, cfg := range rows {
			parts, err := store.Partitions(ctx, cfg.Schema, cfg.Table)
			if err != nil {
				fmt.Fprintf(os.Stderr, "pcs status: %v\n", err)
				return 1
			}
			oldest, newest := "-", "-"
			if len(parts) > 0 {
				oldest, newest = parts[0].Name, parts[len(parts)-1].Name
			}
			keep := fmt.Sprintf("%d", cfg.KeepDays)
			if cfg.KeepDays == 0 {
				keep = "never"
			}
			fmt.Fprintf(w, "%s.%s\t%s\t%s\t%s\t%02d\t%d\t%s\t%s\n",
				cfg.Schema, cfg.Table, cfg.Column, cfg.State, keep, cfg.BoundaryHour, len(parts), oldest, newest)
		}
		w.Flush()
	}

	if *logLines > 0 {
		entries, err := store.RecentLog(ctx, *logLines)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pcs status: %v\n", err)
			return 1
		}
		fmt.Printf("\nrecent pcs_log entries:\n")
		if len(entries) == 0 {
			fmt.Println("  (none)")
		}
		for _, e := range entries {
			fmt.Printf("  %s  %-5s %-8s %-30s %s\n", e.Timestamp, e.Type, e.Proc, e.Target, e.Message)
		}
	}
	return 0
}
