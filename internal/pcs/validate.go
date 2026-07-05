package pcs

import (
	"context"
	"database/sql"
	"fmt"
)

// TableInfo describes a candidate table's partition-relevant properties.
type TableInfo struct {
	Engine   string
	DataType string // of the partition column: datetime or timestamp
	Scheme   Scheme
}

// ValidateTable checks that schema.table with the given column can be
// managed by PCS. Port of the checks scattered across pcs_config_insert
// and run_pcs, plus the InnoDB engine check the legacy edition lacked
// (MySQL 8.0+ supports native InnoDB partitioning only).
func (s *Store) ValidateTable(ctx context.Context, schema, table, column string) (*TableInfo, error) {
	var engine sql.NullString
	err := s.DB.QueryRowContext(ctx,
		"SELECT engine FROM information_schema.tables WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'",
		schema, table).Scan(&engine)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("table %s.%s does not exist", schema, table)
	}
	if err != nil {
		return nil, fmt.Errorf("checking table %s.%s: %w", schema, table, err)
	}
	if !engine.Valid || engine.String != "InnoDB" {
		return nil, fmt.Errorf("table %s.%s uses engine %s; MySQL 8.0+ supports partitioning on InnoDB only",
			schema, table, engine.String)
	}

	var dataType string
	err = s.DB.QueryRowContext(ctx,
		"SELECT data_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?",
		schema, table, column).Scan(&dataType)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("column %s does not exist in %s.%s", column, schema, table)
	}
	if err != nil {
		return nil, fmt.Errorf("checking column %s.%s.%s: %w", schema, table, column, err)
	}

	info := &TableInfo{Engine: engine.String, DataType: dataType}
	switch dataType {
	case "datetime":
		info.Scheme = SchemeToDays
	case "timestamp":
		info.Scheme = SchemeUnixTimestamp
	default:
		return nil, fmt.Errorf("partitioning not allowed on %s datatype (column %s.%s.%s); need datetime or timestamp",
			dataType, schema, table, column)
	}

	// Partitioned tables cannot participate in foreign keys, in either
	// direction (same check as run_pcs).
	var fkCount int
	err = s.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		 AND tc.table_schema = kcu.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
		  AND tc.table_schema = ?
		  AND (tc.table_name = ? OR kcu.referenced_table_name = ?)`,
		schema, table, table).Scan(&fkCount)
	if err != nil {
		return nil, fmt.Errorf("checking foreign keys on %s.%s: %w", schema, table, err)
	}
	if fkCount > 0 {
		return nil, fmt.Errorf("table %s.%s participates in %d foreign key constraint(s); partitioned tables cannot use foreign keys",
			schema, table, fkCount)
	}
	return info, nil
}
