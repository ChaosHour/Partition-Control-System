package pcs

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/ChaosHour/Partition-Control-System/internal/db"
)

// PartitionInfo is one existing partition of a managed table, in
// ordinal order. MAXVALUE partitions carry IsMaxValue.
type PartitionInfo struct {
	Name        string
	Description int64
	IsMaxValue  bool
}

// Partitions returns a table's RANGE partitions in ordinal order, empty
// if the table is not partitioned.
func (s *Store) Partitions(ctx context.Context, schema, table string) ([]PartitionInfo, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT partition_name, partition_description
		FROM information_schema.partitions
		WHERE table_schema = ? AND table_name = ? AND partition_name IS NOT NULL
		ORDER BY partition_ordinal_position`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("listing partitions of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	var out []PartitionInfo
	for rows.Next() {
		var p PartitionInfo
		var desc string
		if err := rows.Scan(&p.Name, &desc); err != nil {
			return nil, err
		}
		if desc == "MAXVALUE" {
			p.IsMaxValue = true
			p.Description = math.MaxInt64
		} else {
			p.Description, err = strconv.ParseInt(desc, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("partition %s of %s.%s has non-numeric boundary %q", p.Name, schema, table, desc)
			}
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ServerToday returns the server's current date (CURDATE()), which is
// what all day walking is based on — the client's clock and timezone
// are never used.
func (s *Store) ServerToday(ctx context.Context) (time.Time, error) {
	var d string
	if err := s.DB.QueryRowContext(ctx, "SELECT CURDATE()").Scan(&d); err != nil {
		return time.Time{}, fmt.Errorf("reading server date: %w", err)
	}
	return time.Parse("2006-01-02", d)
}

// EpochDay converts an epoch-seconds partition boundary to the server's
// calendar date for it (timezone conversion happens on the server).
func (s *Store) EpochDay(ctx context.Context, epoch int64) (time.Time, error) {
	var d string
	if err := s.DB.QueryRowContext(ctx, "SELECT DATE(FROM_UNIXTIME(?))", epoch).Scan(&d); err != nil {
		return time.Time{}, fmt.Errorf("converting epoch %d to server date: %w", epoch, err)
	}
	return time.Parse("2006-01-02", d)
}

// OldestDay returns the server date of the oldest value in the managed
// column, or ok=false for an empty table.
func (s *Store) OldestDay(ctx context.Context, schema, table, column string) (time.Time, bool, error) {
	var d sql.NullString
	q := fmt.Sprintf("SELECT DATE(MIN(%s)) FROM %s", db.QuoteIdent(column), db.QuoteQualified(schema, table))
	if err := s.DB.QueryRowContext(ctx, q).Scan(&d); err != nil {
		return time.Time{}, false, fmt.Errorf("finding oldest %s in %s.%s: %w", column, schema, table, err)
	}
	if !d.Valid {
		return time.Time{}, false, nil
	}
	t, err := time.Parse("2006-01-02", d.String)
	return t, err == nil, err
}

// execDDL runs a DDL statement, or just prints it in dry-run mode.
func (s *Store) execDDL(ctx context.Context, stmt string) error {
	if s.DryRun {
		fmt.Printf("DRY-RUN: %s\n", stmt)
		return nil
	}
	_, err := s.DB.ExecContext(ctx, stmt)
	return err
}
