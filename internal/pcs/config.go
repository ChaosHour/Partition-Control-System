package pcs

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ChaosHour/Partition-Control-System/internal/db"
)

// ConfigRow is one pcs_config entry.
type ConfigRow struct {
	Schema       string
	Table        string
	Column       string
	State        string // Init | Active | Ignore
	KeepDays     uint
	BoundaryHour uint8
}

// KeepDays limits (same clamp range as the legacy pcs_drop, but enforced
// at registration time — review finding F7). 0 means never drop.
const (
	MinKeepDays = 3
	MaxKeepDays = 2048
)

// ValidateKeepDays rejects out-of-range retention instead of silently
// clamping at drop time like the legacy edition.
func ValidateKeepDays(days int) error {
	if days == 0 {
		return nil
	}
	if days < MinKeepDays || days > MaxKeepDays {
		return fmt.Errorf("keep-days must be 0 (never drop) or between %d and %d, got %d",
			MinKeepDays, MaxKeepDays, days)
	}
	return nil
}

// UpsertConfig registers a table (state Init) or, if it is already
// registered, updates keep_days and boundary hour — the legacy
// pcs_config_insert behavior. Returns true when a new row was created.
func (s *Store) UpsertConfig(ctx context.Context, row ConfigRow, comment string) (bool, error) {
	var existingState string
	err := s.DB.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT table_state FROM %s WHERE part_schema = ? AND part_table = ?",
		db.QuoteQualified(s.Schema, "pcs_config")),
		row.Schema, row.Table).Scan(&existingState)
	switch {
	case err == sql.ErrNoRows:
		_, err = s.DB.ExecContext(ctx, fmt.Sprintf(
			`INSERT INTO %s (part_schema, part_table, partition_column, table_state, keep_days, partition_boundary_hour, comment)
			 VALUES (?, ?, ?, 'Init', ?, ?, ?)`,
			db.QuoteQualified(s.Schema, "pcs_config")),
			row.Schema, row.Table, row.Column, row.KeepDays, row.BoundaryHour, comment)
		if err != nil {
			return false, fmt.Errorf("inserting pcs_config row for %s.%s: %w", row.Schema, row.Table, err)
		}
		return true, nil
	case err != nil:
		return false, fmt.Errorf("checking pcs_config for %s.%s: %w", row.Schema, row.Table, err)
	default:
		_, err = s.DB.ExecContext(ctx, fmt.Sprintf(
			`UPDATE %s SET keep_days = ?, partition_boundary_hour = ?, comment = ?
			 WHERE part_schema = ? AND part_table = ?`,
			db.QuoteQualified(s.Schema, "pcs_config")),
			row.KeepDays, row.BoundaryHour, comment, row.Schema, row.Table)
		if err != nil {
			return false, fmt.Errorf("updating pcs_config row for %s.%s: %w", row.Schema, row.Table, err)
		}
		return false, nil
	}
}

// ConfigRows returns all pcs_config entries, ordered by schema and table.
func (s *Store) ConfigRows(ctx context.Context) ([]ConfigRow, error) {
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(
		`SELECT part_schema, part_table, partition_column, table_state, keep_days, partition_boundary_hour
		 FROM %s ORDER BY part_schema, part_table`,
		db.QuoteQualified(s.Schema, "pcs_config")))
	if err != nil {
		return nil, fmt.Errorf("reading pcs_config: %w", err)
	}
	defer rows.Close()

	var out []ConfigRow
	for rows.Next() {
		var r ConfigRow
		if err := rows.Scan(&r.Schema, &r.Table, &r.Column, &r.State, &r.KeepDays, &r.BoundaryHour); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SetTableState updates the state of one registered table.
func (s *Store) SetTableState(ctx context.Context, schema, table, state string) error {
	_, err := s.DB.ExecContext(ctx, fmt.Sprintf(
		"UPDATE %s SET table_state = ? WHERE part_schema = ? AND part_table = ?",
		db.QuoteQualified(s.Schema, "pcs_config")),
		state, schema, table)
	if err != nil {
		return fmt.Errorf("setting state %s on %s.%s: %w", state, schema, table, err)
	}
	return nil
}
