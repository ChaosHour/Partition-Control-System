package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/ChaosHour/Partition-Control-System/internal/db"
	"github.com/ChaosHour/Partition-Control-System/internal/pcs"
)

// infoSnapshot is the -json document: one run over one or more tables,
// stamped with the server it came from so before/after files are
// self-describing.
type infoSnapshot struct {
	ServerVersion string             `json:"server_version"`
	GeneratedAt   string             `json:"generated_at"` // server NOW()
	Exact         bool               `json:"exact"`
	Tables        []*pcs.TableReport `json:"tables"`
}

// cmdInfo reports everything about a table's partitions: layout, sizes,
// boundaries, row counts and the data range of the partition column.
// Made for before/after validation around upgrades:
//
//	pcs info -json -exact > before.json   # ... upgrade ...
//	pcs info -json -exact > after.json
//	diff before.json after.json
func cmdInfo(args []string) int {
	fs := flag.NewFlagSet("pcs info", flag.ExitOnError)
	var conn db.ConnFlags
	conn.Register(fs)
	schema := fs.String("schema", pcs.DefaultSchema, "PCS metadata database (excluded from discovery)")
	database := fs.String("database", "", "only this database (default: every partitioned table)")
	table := fs.String("table", "", "only this table (requires -database)")
	exact := fs.Bool("exact", false, "exact COUNT(*)/MIN/MAX per partition (scans each partition; slow on big tables)")
	asJSON := fs.Bool("json", false, "JSON output (machine-diffable snapshot)")
	fs.Parse(args)

	fail := func(format string, a ...any) int {
		fmt.Fprintf(os.Stderr, "pcs info: "+format+"\n", a...)
		return 1
	}
	if *table != "" && *database == "" {
		fmt.Fprintln(os.Stderr, "pcs info: -table requires -database")
		fs.Usage()
		return 2
	}

	ctx := context.Background()
	d, err := conn.Connect(ctx)
	if err != nil {
		return fail("%v", err)
	}
	defer d.Close()
	store := &pcs.Store{DB: d, Schema: *schema}

	var targets [][2]string
	if *table != "" {
		targets = [][2]string{{*database, *table}}
	} else {
		if targets, err = store.ListPartitionedTables(ctx, *database); err != nil {
			return fail("%v", err)
		}
		if len(targets) == 0 {
			return fail("no partitioned tables found")
		}
	}

	snap := infoSnapshot{ServerVersion: d.Version.Raw, Exact: *exact}
	if err := d.QueryRowContext(ctx, "SELECT NOW()").Scan(&snap.GeneratedAt); err != nil {
		return fail("reading server time: %v", err)
	}
	for _, t := range targets {
		r, err := store.Inspect(ctx, t[0], t[1], *exact)
		if err != nil {
			return fail("%v", err)
		}
		snap.Tables = append(snap.Tables, r)
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(snap); err != nil {
			return fail("%v", err)
		}
		return 0
	}

	fmt.Printf("server %s, time %s\n", snap.ServerVersion, snap.GeneratedAt)
	for _, r := range snap.Tables {
		printTableReport(r, *exact)
	}
	if !*exact {
		fmt.Println("\nrow counts are information_schema estimates; use -exact for real COUNT(*)/MIN/MAX per partition")
	}
	return 0
}

func printTableReport(r *pcs.TableReport, exact bool) {
	fmt.Printf("\n== %s.%s ==\n", r.Schema, r.Table)
	scheme := r.Scheme
	if scheme == "" {
		scheme = "unmanaged"
	}
	fmt.Printf("engine %s, method %s (%s), column %s, scheme %s\n",
		r.Engine, r.Method, orDash(r.Expression), orDash(r.Column), scheme)
	fmt.Printf("partitions %d, rows %s, data %s, index %s",
		r.PartitionCount, pcs.Comma(r.RowsEstimate), pcs.HumanBytes(r.DataBytes), pcs.HumanBytes(r.IndexBytes))
	if r.AutoIncrement != nil {
		fmt.Printf(", auto_increment %s", pcs.Comma(*r.AutoIncrement))
	}
	fmt.Println()
	if r.Min != "" || r.Max != "" {
		fmt.Printf("%s range: %s .. %s\n", r.Column, orDash(r.Min), orDash(r.Max))
	}
	if r.CreateTime != "" {
		fmt.Printf("created %s", r.CreateTime)
		if r.UpdateTime != "" {
			fmt.Printf(", last updated %s", r.UpdateTime)
		}
		fmt.Println()
	}

	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	if exact {
		fmt.Fprintln(w, "\nPARTITION\tBOUNDARY (<)\tROWS\tDATA\tINDEX\tMIN\tMAX")
	} else {
		fmt.Fprintln(w, "\nPARTITION\tBOUNDARY (<)\tROWS~\tDATA\tINDEX")
	}
	for _, p := range r.Partitions {
		boundary := p.Boundary
		switch {
		case p.BoundaryTime != "" && p.Boundary == "'"+p.BoundaryTime+"'":
			boundary = p.BoundaryTime // quoted literal: skip the redundant raw form
		case p.BoundaryTime != "":
			boundary = fmt.Sprintf("%s (%s)", p.BoundaryTime, p.Boundary)
		}
		if exact {
			rows := "-"
			if p.ExactRows != nil {
				rows = pcs.Comma(*p.ExactRows)
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", p.Name, boundary, rows,
				pcs.HumanBytes(p.DataBytes), pcs.HumanBytes(p.IndexBytes), orDash(p.Min), orDash(p.Max))
		} else {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", p.Name, boundary,
				pcs.Comma(p.RowsEstimate), pcs.HumanBytes(p.DataBytes), pcs.HumanBytes(p.IndexBytes))
		}
	}
	w.Flush()

	fmt.Printf("\n%s;\n", r.CreateTable)
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
