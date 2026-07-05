package cli

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/ChaosHour/Partition-Control-System/internal/db"
	"github.com/ChaosHour/Partition-Control-System/internal/pcs"
)

// cmdRun processes every registered table: partitions Init tables,
// enforces retention and extends coverage on Active tables (port of the
// run_pcs procedure plus the daily event's replica check). Designed to
// be run from cron/systemd/Cloud Scheduler once a day.
func cmdRun(args []string) int {
	fs := flag.NewFlagSet("pcs run", flag.ExitOnError)
	var conn db.ConnFlags
	conn.Register(fs)
	schema := fs.String("schema", pcs.DefaultSchema, "database holding PCS metadata")
	dryRun := fs.Bool("dry-run", false, "print DDL and log entries instead of executing")
	fs.Parse(args)

	ctx := context.Background()
	d, err := conn.Connect(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pcs run: %v\n", err)
		return 1
	}
	defer d.Close()

	// Replica no-op: partition DDL replicates from the source. This is
	// the check the legacy event lost on MySQL 8.0+ (finding F3).
	replica, reason, err := d.IsReplica(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pcs run: %v\n", err)
		return 1
	}
	if replica {
		fmt.Printf("instance is a replica (%s); skipping run\n", reason)
		return 0
	}

	store := &pcs.Store{DB: d, Schema: *schema, DryRun: *dryRun}

	// One run at a time per server.
	lockName := "pcs_run_" + *schema
	var got int
	if err := d.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", lockName).Scan(&got); err != nil || got != 1 {
		fmt.Fprintf(os.Stderr, "pcs run: another run appears to be in progress (lock %s not acquired)\n", lockName)
		return 1
	}
	defer d.ExecContext(ctx, "DO RELEASE_LOCK(?)", lockName)

	today, err := store.ServerToday(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pcs run: %v\n", err)
		return 1
	}

	rows, err := store.ConfigRows(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pcs run: %v\n", err)
		return 1
	}
	if len(rows) == 0 {
		fmt.Println("nothing to do: no tables registered in pcs_config")
		return 0
	}

	store.Info(ctx, "Run", "", "", "", "starting pcs run "+version)
	failures := 0
	for _, cfg := range rows {
		if cfg.State == "Ignore" {
			continue
		}

		info, err := store.ValidateTable(ctx, cfg.Schema, cfg.Table, cfg.Column)
		if err != nil {
			fmt.Fprintf(os.Stderr, "SKIP  %s.%s: %v\n", cfg.Schema, cfg.Table, err)
			store.Error(ctx, "Run", cfg.Schema, cfg.Table, "", err.Error())
			failures++
			continue
		}

		switch cfg.State {
		case "Init":
			if err := store.InitTable(ctx, cfg, info, today); err != nil {
				fmt.Fprintf(os.Stderr, "FAIL  %s.%s: %v\n", cfg.Schema, cfg.Table, err)
				failures++
				continue
			}
			fmt.Printf("INIT  %s.%s partitioned (%s on %s)\n", cfg.Schema, cfg.Table, info.Scheme, cfg.Column)
			if *dryRun {
				continue // table is not actually partitioned; extend would fail
			}
			added, err := store.ExtendTable(ctx, cfg, info, today)
			if err != nil {
				fmt.Fprintf(os.Stderr, "FAIL  %s.%s: %v\n", cfg.Schema, cfg.Table, err)
				failures++
				continue
			}
			fmt.Printf("ADD   %s.%s: %d partition(s) added\n", cfg.Schema, cfg.Table, added)

		case "Active":
			dropped, err := store.EnforceRetention(ctx, cfg, info, today)
			if err != nil {
				fmt.Fprintf(os.Stderr, "FAIL  %s.%s: %v\n", cfg.Schema, cfg.Table, err)
				failures++
				continue
			}
			if dropped > 0 {
				fmt.Printf("DROP  %s.%s: %d partition(s) dropped (keep_days %d)\n", cfg.Schema, cfg.Table, dropped, cfg.KeepDays)
			}
			added, err := store.ExtendTable(ctx, cfg, info, today)
			if err != nil {
				fmt.Fprintf(os.Stderr, "FAIL  %s.%s: %v\n", cfg.Schema, cfg.Table, err)
				failures++
				continue
			}
			if added > 0 {
				fmt.Printf("ADD   %s.%s: %d partition(s) added\n", cfg.Schema, cfg.Table, added)
			} else {
				fmt.Printf("OK    %s.%s: coverage sufficient\n", cfg.Schema, cfg.Table)
			}

		default:
			fmt.Fprintf(os.Stderr, "SKIP  %s.%s: unknown state %q\n", cfg.Schema, cfg.Table, cfg.State)
			failures++
		}
	}
	store.Info(ctx, "Run", "", "", "", fmt.Sprintf("finished pcs run %s (%d failure(s))", version, failures))

	if failures > 0 {
		fmt.Fprintf(os.Stderr, "pcs run: completed with %d failure(s)\n", failures)
		return 1
	}
	return 0
}
