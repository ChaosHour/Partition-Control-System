package cli

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ChaosHour/Partition-Control-System/internal/db"
	"github.com/ChaosHour/Partition-Control-System/internal/pcs"
)

// cmdCheck verifies partition coverage and retention for every
// registered table (port of pcs_check, with per-scheme date rendering —
// the legacy procedure printed FROM_DAYS() of unix timestamps for
// timestamp tables, review finding F4). Exit codes are Nagios-style:
// 0 OK, 1 warning, 2 critical, so it drops into check_mk/Nagios/cron
// alerting directly, replacing the check_db_partition_manager wrapper.
func cmdCheck(args []string) int {
	fs := flag.NewFlagSet("pcs check", flag.ExitOnError)
	var conn db.ConnFlags
	conn.Register(fs)
	schema := fs.String("schema", pcs.DefaultSchema, "database holding PCS metadata")
	fs.Parse(args)

	ctx := context.Background()
	d, err := conn.Connect(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pcs check: %v\n", err)
		return 2
	}
	defer d.Close()

	store := &pcs.Store{DB: d, Schema: *schema}
	today, err := store.ServerToday(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pcs check: %v\n", err)
		return 2
	}
	rows, err := store.ConfigRows(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pcs check: %v\n", err)
		return 2
	}

	// A table is healthy when its newest partition covers through at
	// least today+29 — i.e. the daily run has been extending coverage.
	wantCover := today.AddDate(0, 0, pcs.CreateHorizonDays-1)

	worst := 0
	report := func(level int, format string, a ...any) {
		tag := [3]string{"OK  ", "WARN", "CRIT"}[level]
		fmt.Printf("%s %s\n", tag, fmt.Sprintf(format, a...))
		if level > worst {
			worst = level
		}
	}

	checked := 0
	for _, cfg := range rows {
		if cfg.State == "Ignore" {
			continue
		}
		checked++
		name := cfg.Schema + "." + cfg.Table

		if cfg.State == "Init" {
			report(1, "%s: state Init - awaiting first pcs run", name)
			continue
		}

		parts, err := store.Partitions(ctx, cfg.Schema, cfg.Table)
		if err != nil {
			report(2, "%s: %v", name, err)
			continue
		}
		if len(parts) == 0 {
			report(2, "%s: state Active but table is not partitioned", name)
			continue
		}
		newest := parts[len(parts)-1]
		oldest := parts[0]
		if newest.IsMaxValue {
			report(1, "%s: has a MAXVALUE partition; pcs cannot extend coverage", name)
			continue
		}

		var expr string
		if err := d.QueryRowContext(ctx, `
			SELECT partition_expression FROM information_schema.partitions
			WHERE table_schema = ? AND table_name = ? AND partition_name IS NOT NULL LIMIT 1`,
			cfg.Schema, cfg.Table).Scan(&expr); err != nil {
			report(2, "%s: reading partition expression: %v", name, err)
			continue
		}
		_, scheme := pcs.ParsePartitionExpression(expr)

		// Convert newest/oldest boundaries to dates, per scheme (F4).
		var coversThrough, oldestBoundaryDay time.Time
		switch scheme {
		case pcs.SchemeToDays:
			coversThrough = pcs.FromDays(newest.Description).AddDate(0, 0, -1)
			oldestBoundaryDay = pcs.FromDays(oldest.Description)
		case pcs.SchemeUnixTimestamp:
			day, err := store.EpochDay(ctx, newest.Description)
			if err != nil {
				report(2, "%s: %v", name, err)
				continue
			}
			coversThrough = day.AddDate(0, 0, -1)
			if oldestBoundaryDay, err = store.EpochDay(ctx, oldest.Description); err != nil {
				report(2, "%s: %v", name, err)
				continue
			}
		default:
			report(1, "%s: unmanaged partition expression %q", name, expr)
			continue
		}

		if coversThrough.Before(wantCover) {
			report(2, "%s: coverage ends %s, want >= %s - is the daily pcs run working?",
				name, coversThrough.Format("2006-01-02"), wantCover.Format("2006-01-02"))
			continue
		}

		// Retention-window checks only make sense once the table has
		// accumulated keep_days of history (steady state); a freshly
		// onboarded table legitimately has less.
		if cfg.KeepDays > 0 && len(parts) >= int(cfg.KeepDays)+pcs.CreateHorizonDays {
			// Oldest partition should reach back near the keep window;
			// younger means data was lost or dropped too aggressively.
			cutoff := today.AddDate(0, 0, -int(cfg.KeepDays)+1)
			if oldestBoundaryDay.After(cutoff) {
				report(1, "%s: oldest partition %s starts after keep window (%s, keep_days %d)",
					name, oldest.Name, cutoff.Format("2006-01-02"), cfg.KeepDays)
				continue
			}
			// Far more partitions than keep_days+horizon means
			// retention has stopped dropping.
			if len(parts) > int(cfg.KeepDays)+pcs.CreateHorizonDays+3 {
				report(1, "%s: %d partitions for keep_days %d - is retention running?",
					name, len(parts), cfg.KeepDays)
				continue
			}
		}

		report(0, "%s: %d partitions, coverage through %s (%s)",
			name, len(parts), coversThrough.Format("2006-01-02"), scheme)
	}

	if checked == 0 {
		fmt.Println("OK   no tables registered for checking")
	}
	return worst
}
