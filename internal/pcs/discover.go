package pcs

import (
	"context"
	"fmt"

	"github.com/ChaosHour/Partition-Control-System/internal/db"
)

// DiscoveredTable is a RANGE-partitioned table found on the server.
// Managed reports whether its partition expression uses a scheme PCS
// understands (TO_DAYS or UNIX_TIMESTAMP); unmanaged tables are reported
// but never registered.
type DiscoveredTable struct {
	Schema     string
	Table      string
	Column     string
	Expression string
	Scheme     Scheme
	Managed    bool
}

// DiscoverPartitionedTables lists RANGE-partitioned tables outside the
// system schemas and the PCS metadata schema. Port of the seeding query
// in the legacy pcs_tables procedure, with the column/scheme extraction
// done in Go instead of nested REPLACE/SUBSTRING_INDEX.
func (s *Store) DiscoverPartitionedTables(ctx context.Context) ([]DiscoveredTable, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT table_schema, table_name, partition_expression
		FROM information_schema.partitions
		WHERE partition_name IS NOT NULL
		  AND partition_method = 'RANGE'
		  AND table_schema NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')
		  AND table_schema <> ?
		GROUP BY table_schema, table_name, partition_expression
		ORDER BY table_schema, table_name`, s.Schema)
	if err != nil {
		return nil, fmt.Errorf("discovering partitioned tables: %w", err)
	}
	defer rows.Close()

	var found []DiscoveredTable
	for rows.Next() {
		var t DiscoveredTable
		if err := rows.Scan(&t.Schema, &t.Table, &t.Expression); err != nil {
			return nil, err
		}
		t.Column, t.Scheme = ParsePartitionExpression(t.Expression)
		t.Managed = t.Scheme != SchemeUnknown
		found = append(found, t)
	}
	return found, rows.Err()
}

// RegisterTable inserts a row into pcs_config if one does not already
// exist for schema.table (the legacy INSERT IGNORE behavior). Returns
// whether a new row was inserted.
func (s *Store) RegisterTable(ctx context.Context, schema, table, column, state string, keepDays uint, boundaryHour uint8, comment string) (bool, error) {
	res, err := s.DB.ExecContext(ctx, fmt.Sprintf(
		`INSERT IGNORE INTO %s
		   (part_schema, part_table, partition_column, table_state, keep_days, partition_boundary_hour, comment)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		db.QuoteQualified(s.Schema, "pcs_config")),
		schema, table, column, state, keepDays, boundaryHour, comment)
	if err != nil {
		return false, fmt.Errorf("registering %s.%s: %w", schema, table, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}
