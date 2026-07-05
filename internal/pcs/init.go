package pcs

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ChaosHour/Partition-Control-System/internal/db"
)

// EnsurePrimaryKeyIncludes adds column to the table's primary key if it
// is not already part of it (port of pcs_update_index). MySQL requires
// the partition column in every unique key. Returns the resulting key
// column list when a rebuild happened.
//
// NOTE: on a large table this is a full table rebuild. The legacy
// procedure did the same "without warning!" (its words); here the run
// output and pcs_log record it.
func (s *Store) EnsurePrimaryKeyIncludes(ctx context.Context, schema, table, column string) (rebuilt bool, keyList string, err error) {
	// Existing PK columns in index order (the legacy proc read them
	// unordered from information_schema.columns, which could scramble
	// composite keys on rebuild).
	rows, err := s.DB.QueryContext(ctx, `
		SELECT column_name FROM information_schema.statistics
		WHERE table_schema = ? AND table_name = ? AND index_name = 'PRIMARY'
		ORDER BY seq_in_index`, schema, table)
	if err != nil {
		return false, "", fmt.Errorf("reading primary key of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	var pkCols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return false, "", err
		}
		pkCols = append(pkCols, c)
	}
	if err := rows.Err(); err != nil {
		return false, "", err
	}

	if slices.Contains(pkCols, column) {
		return false, "", nil // already part of the PK
	}

	var alter string
	quoted := make([]string, 0, len(pkCols)+1)
	for _, c := range pkCols {
		quoted = append(quoted, db.QuoteIdent(c))
	}
	quoted = append(quoted, db.QuoteIdent(column))
	keyList = strings.Join(quoted, ", ")
	if len(pkCols) == 0 {
		alter = fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s)", db.QuoteQualified(schema, table), keyList)
	} else {
		alter = fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY, ADD PRIMARY KEY (%s)", db.QuoteQualified(schema, table), keyList)
	}
	if err := s.execDDL(ctx, alter); err != nil {
		return false, "", fmt.Errorf("rebuilding primary key of %s.%s: %w", schema, table, err)
	}
	return true, keyList, nil
}

// InitTable performs the initial PARTITION BY RANGE of a registered
// table (port of pcs_init): PK adjustment, partitions from the oldest
// data day through tomorrow, then state Init -> Active.
func (s *Store) InitTable(ctx context.Context, cfg ConfigRow, info *TableInfo, today time.Time) error {
	parts, err := s.Partitions(ctx, cfg.Schema, cfg.Table)
	if err != nil {
		return err
	}
	if len(parts) > 0 {
		// Already partitioned. If it matches the configured column and
		// a scheme we manage, adopt it (state Active) instead of
		// erroring forever like the legacy procedure did.
		var expr string
		err := s.DB.QueryRowContext(ctx, `
			SELECT partition_expression FROM information_schema.partitions
			WHERE table_schema = ? AND table_name = ? AND partition_name IS NOT NULL
			  AND partition_method = 'RANGE' LIMIT 1`, cfg.Schema, cfg.Table).Scan(&expr)
		if err != nil {
			return fmt.Errorf("%s.%s is already partitioned but not by RANGE; cannot manage", cfg.Schema, cfg.Table)
		}
		col, scheme := ParsePartitionExpression(expr)
		if col != cfg.Column || scheme != info.Scheme {
			return fmt.Errorf("%s.%s is already partitioned by %q, not by %s(%s); set state Ignore or fix pcs_config",
				cfg.Schema, cfg.Table, expr, info.Scheme, cfg.Column)
		}
		s.Info(ctx, "Init", cfg.Schema, cfg.Table, "", "already partitioned with matching scheme; adopting as Active")
		return s.SetTableState(ctx, cfg.Schema, cfg.Table, "Active")
	}

	rebuilt, keyList, err := s.EnsurePrimaryKeyIncludes(ctx, cfg.Schema, cfg.Table, cfg.Column)
	if err != nil {
		s.Error(ctx, "Init", cfg.Schema, cfg.Table, "", err.Error())
		return err
	}
	if rebuilt {
		s.Info(ctx, "Init", cfg.Schema, cfg.Table, "", "PRIMARY KEY rebuilt to ("+keyList+")")
	}

	oldest, ok, err := s.OldestDay(ctx, cfg.Schema, cfg.Table, cfg.Column)
	if err != nil {
		return err
	}
	if !ok {
		oldest = today.AddDate(0, 0, -3) // empty table: legacy default of 3 days back
	}

	specs := PlanInit(info.Scheme, oldest, today, int(cfg.BoundaryHour))
	var exprFn string
	switch info.Scheme {
	case SchemeToDays:
		exprFn = fmt.Sprintf("TO_DAYS(%s)", db.QuoteIdent(cfg.Column))
	case SchemeUnixTimestamp:
		exprFn = fmt.Sprintf("UNIX_TIMESTAMP(%s)", db.QuoteIdent(cfg.Column))
	}
	defs := make([]string, len(specs))
	for i, sp := range specs {
		defs[i] = fmt.Sprintf("PARTITION %s VALUES LESS THAN (%s)", sp.Name, sp.Boundary)
	}
	alter := fmt.Sprintf("ALTER TABLE %s PARTITION BY RANGE (%s) (%s)",
		db.QuoteQualified(cfg.Schema, cfg.Table), exprFn, strings.Join(defs, ", "))

	if err := s.execDDL(ctx, alter); err != nil {
		msg := fmt.Sprintf("initial partitioning failed: %v", err)
		s.Error(ctx, "Init", cfg.Schema, cfg.Table, "", msg)
		return fmt.Errorf("partitioning %s.%s: %w", cfg.Schema, cfg.Table, err)
	}
	s.Info(ctx, "Init", cfg.Schema, cfg.Table, specs[len(specs)-1].Name,
		fmt.Sprintf("table partitioned: %d initial partitions (%s .. %s), scheme %s",
			len(specs), specs[0].Name, specs[len(specs)-1].Name, info.Scheme))
	if s.DryRun {
		return nil
	}
	return s.SetTableState(ctx, cfg.Schema, cfg.Table, "Active")
}
