package cli

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/ChaosHour/Partition-Control-System/internal/db"
	"github.com/ChaosHour/Partition-Control-System/internal/pcs"
)

// cmdAdd registers a table for partition management (port of the legacy
// pcs_config_insert procedure). New tables enter state Init and get
// partitioned on the next pcs run; re-adding an existing table updates
// its keep_days and boundary hour.
func cmdAdd(args []string) int {
	fs := flag.NewFlagSet("pcs add", flag.ExitOnError)
	var conn db.ConnFlags
	conn.Register(fs)
	schema := fs.String("schema", pcs.DefaultSchema, "database holding PCS metadata")
	database := fs.String("database", "", "database of the table to manage (required)")
	table := fs.String("table", "", "table to manage (required)")
	column := fs.String("column", "", "datetime/timestamp column to partition on (required)")
	keepDays := fs.Int("keep-days", 0, "days of partitions to keep; 0 = never drop (else 3-2048)")
	boundaryHour := fs.Int("boundary-hour", 0, "hour of day (0-23) partitions roll over (timestamp columns only)")
	fs.Parse(args)

	fail := func(format string, a ...any) int {
		fmt.Fprintf(os.Stderr, "pcs add: "+format+"\n", a...)
		return 1
	}

	if *database == "" || *table == "" || *column == "" {
		fmt.Fprintln(os.Stderr, "pcs add: -database, -table and -column are required")
		fs.Usage()
		return 2
	}
	if err := pcs.ValidateKeepDays(*keepDays); err != nil {
		return fail("%v", err)
	}
	if *boundaryHour < 0 || *boundaryHour > 23 {
		return fail("boundary-hour must be 0-23, got %d", *boundaryHour)
	}

	ctx := context.Background()
	d, err := conn.Connect(ctx)
	if err != nil {
		return fail("%v", err)
	}
	defer d.Close()

	store := &pcs.Store{DB: d, Schema: *schema}

	info, err := store.ValidateTable(ctx, *database, *table, *column)
	if err != nil {
		// Best-effort audit of the rejection, matching legacy behavior.
		store.Error(ctx, "Add", *database, *table, "", err.Error())
		return fail("%v", err)
	}

	row := pcs.ConfigRow{
		Schema:       *database,
		Table:        *table,
		Column:       *column,
		KeepDays:     uint(*keepDays),
		BoundaryHour: uint8(*boundaryHour),
	}
	created, err := store.UpsertConfig(ctx, row, "added by pcs add")
	if err != nil {
		return fail("%v", err)
	}

	if created {
		fmt.Printf("registered %s.%s (%s on %s, scheme %s, keep_days %d) - state Init; next 'pcs run' will partition it\n",
			*database, *table, info.DataType, *column, info.Scheme, *keepDays)
		if err := store.Info(ctx, "Add", *database, *table, "", fmt.Sprintf("registered (Init, keep_days %d)", *keepDays)); err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: audit log entry failed: %v\n", err)
		}
	} else {
		fmt.Printf("updated %s.%s (keep_days %d, boundary hour %02d)\n", *database, *table, *keepDays, *boundaryHour)
		if err := store.Info(ctx, "Add", *database, *table, "", fmt.Sprintf("updated (keep_days %d, boundary hour %d)", *keepDays, *boundaryHour)); err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: audit log entry failed: %v\n", err)
		}
	}
	return 0
}
