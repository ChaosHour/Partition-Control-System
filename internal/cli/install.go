package cli

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/ChaosHour/Partition-Control-System/internal/db"
	"github.com/ChaosHour/Partition-Control-System/internal/pcs"
)

// cmdInstall bootstraps the PCS metadata schema (database + pcs_config +
// pcs_log). Replaces the Ansible role and the schema portion of pcs.sql.
// Idempotent: safe to re-run; never alters existing objects.
func cmdInstall(args []string) int {
	fs := flag.NewFlagSet("pcs install", flag.ExitOnError)
	var conn db.ConnFlags
	conn.Register(fs)
	schema := fs.String("schema", pcs.DefaultSchema, "database to hold PCS metadata")
	discover := fs.Bool("discover", false, "register already-partitioned tables in pcs_config (state Active, keep_days 0 = never drop)")
	fs.Parse(args)

	ctx := context.Background()
	d, err := conn.Connect(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pcs install: %v\n", err)
		return 1
	}
	defer d.Close()

	// Installing on a replica would either fail (read_only) or, worse,
	// write DDL that conflicts with what replicates from the source.
	replica, reason, err := d.IsReplica(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pcs install: %v\n", err)
		return 1
	}
	if replica {
		fmt.Fprintf(os.Stderr, "pcs install: refusing to install on a replica (%s); install on the source and let it replicate\n", reason)
		return 1
	}

	store := &pcs.Store{DB: d, Schema: *schema}

	warns, err := store.LegacyWarnings(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pcs install: %v\n", err)
		return 1
	}
	for _, w := range warns {
		fmt.Fprintf(os.Stderr, "WARNING: %s\n", w)
	}

	if err := store.EnsureSchema(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "pcs install: %v\n", err)
		return 1
	}
	// Audit logging is best-effort here: with legacy-layout tables the
	// ENUM column rejects the Go edition's values under strict mode, but
	// the schema bootstrap itself has already succeeded.
	if err := store.Info(ctx, "Install", "", "", "", "pcs install completed (version "+version+", server "+d.Version.Raw+")"); err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: schema ready but audit log entry failed: %v\n", err)
	}

	fmt.Printf("pcs metadata schema ready in %s (server %s)\n", *schema, d.Version.Raw)

	found, err := store.DiscoverPartitionedTables(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pcs install: %v\n", err)
		return 1
	}
	if !*discover {
		if managed := managedCount(found); managed > 0 {
			fmt.Printf("%d RANGE-partitioned table(s) found that pcs could manage; re-run with -discover to register them\n", managed)
		}
		return 0
	}

	for _, t := range found {
		if !t.Managed {
			fmt.Printf("skipped    %s.%s: partition expression %q is not a TO_DAYS/UNIX_TIMESTAMP scheme\n", t.Schema, t.Table, t.Expression)
			continue
		}
		inserted, err := store.RegisterTable(ctx, t.Schema, t.Table, t.Column, "Active", 0, 0,
			"discovered by pcs install -discover")
		if err != nil {
			fmt.Fprintf(os.Stderr, "pcs install: %v\n", err)
			return 1
		}
		if inserted {
			fmt.Printf("registered %s.%s (%s on %s, keep_days 0 = never drop)\n", t.Schema, t.Table, t.Scheme, t.Column)
			if err := store.Info(ctx, "Discover", t.Schema, t.Table, "", "registered as Active by install -discover"); err != nil {
				fmt.Fprintf(os.Stderr, "WARNING: audit log entry failed: %v\n", err)
			}
		} else {
			fmt.Printf("exists     %s.%s (already in pcs_config)\n", t.Schema, t.Table)
		}
	}
	return 0
}

func managedCount(found []pcs.DiscoveredTable) int {
	n := 0
	for _, t := range found {
		if t.Managed {
			n++
		}
	}
	return n
}
