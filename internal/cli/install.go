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
	return 0
}
