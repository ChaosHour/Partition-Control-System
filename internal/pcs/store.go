// Package pcs implements the Partition Control System domain logic: the
// metadata schema (pcs_config, pcs_log), audit logging, and — as the port
// progresses — table registration, partition creation and retention.
package pcs

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ChaosHour/Partition-Control-System/internal/db"
)

// DefaultSchema is the database holding PCS metadata, matching the
// stored-procedure edition so both editions are found in the same place.
const DefaultSchema = "pcs"

// Store is the PCS metadata store: pcs_config and pcs_log inside Schema.
type Store struct {
	DB     *db.DB
	Schema string
}

// Modernized table definitions (5.7 through 9.x safe): utf8mb4 instead of
// deprecated utf8, no integer display widths, and pcs_log gains a real
// primary key (the original had none, which row-based replication and
// online-DDL tools dislike). logging_proc/message_type are varchar, not
// ENUM — the original triggers inserted values missing from the ENUM,
// which strict mode rejects (review finding F2); varchar removes the trap.
const (
	logTable = `CREATE TABLE IF NOT EXISTS %s (
  id bigint unsigned NOT NULL AUTO_INCREMENT,
  part_schema varchar(64) DEFAULT NULL,
  part_table varchar(64) DEFAULT NULL,
  part_partition varchar(64) DEFAULT NULL,
  message_type varchar(8) NOT NULL DEFAULT 'Info',
  logging_proc varchar(24) NOT NULL DEFAULT 'Run',
  action_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  message varchar(512) NOT NULL,
  PRIMARY KEY (id),
  KEY idx_action_timestamp (action_timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	configTable = `CREATE TABLE IF NOT EXISTS %s (
  part_schema varchar(64) NOT NULL COMMENT 'Database of the table',
  part_table varchar(64) NOT NULL COMMENT 'The partitioned table',
  partition_column varchar(64) NOT NULL COMMENT 'Table column partitioned',
  table_state enum('Init','Active','Ignore') NOT NULL DEFAULT 'Init' COMMENT 'Init: to be partitioned. Active: partitioned. Ignore: no processing.',
  keep_days int unsigned NOT NULL DEFAULT 0 COMMENT 'Days to keep. Min 3, Max 2048, 0 = never drop',
  comment varchar(512) DEFAULT NULL,
  last_update timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  partition_boundary_hour tinyint unsigned NOT NULL DEFAULT 0 COMMENT 'Hour of day (0-23) for switching to the next partition',
  PRIMARY KEY (part_schema, part_table)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
)

// EnsureSchema creates the metadata database and tables if missing.
// Idempotent; never alters existing objects.
func (s *Store) EnsureSchema(ctx context.Context) error {
	stmts := []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET utf8mb4", db.QuoteIdent(s.Schema)),
		fmt.Sprintf(logTable, db.QuoteQualified(s.Schema, "pcs_log")),
		fmt.Sprintf(configTable, db.QuoteQualified(s.Schema, "pcs_config")),
	}
	for _, stmt := range stmts {
		if _, err := s.DB.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensuring pcs schema: %w", err)
		}
	}
	return nil
}

// LegacyWarnings inspects the metadata schema for artifacts of the
// stored-procedure edition and returns human-readable warnings. The Go
// edition coexists with it in the repo, but both must not manage the same
// tables concurrently.
func (s *Store) LegacyWarnings(ctx context.Context) ([]string, error) {
	var warns []string

	var eventStatus string
	err := s.DB.QueryRowContext(ctx,
		"SELECT status FROM information_schema.events WHERE event_schema = ? AND event_name = 'pcs_event'",
		s.Schema).Scan(&eventStatus)
	switch {
	case err == sql.ErrNoRows:
	case err != nil:
		return nil, fmt.Errorf("checking for legacy event: %w", err)
	case eventStatus == "ENABLED":
		warns = append(warns, fmt.Sprintf(
			"legacy stored-procedure event %s.pcs_event is ENABLED - do not let both editions manage the same tables (disable with: ALTER EVENT %s.pcs_event DISABLE)",
			s.Schema, s.Schema))
	default:
		warns = append(warns, fmt.Sprintf(
			"legacy stored-procedure event %s.pcs_event exists (status %s)", s.Schema, eventStatus))
	}

	var logProcType string
	err = s.DB.QueryRowContext(ctx,
		"SELECT data_type FROM information_schema.columns WHERE table_schema = ? AND table_name = 'pcs_log' AND column_name = 'logging_proc'",
		s.Schema).Scan(&logProcType)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("checking pcs_log shape: %w", err)
	}
	if logProcType == "enum" {
		warns = append(warns, fmt.Sprintf(
			"%s.pcs_log has the legacy ENUM layout; under strict SQL mode the Go edition's log entries may be rejected - consider installing into a separate schema (-schema pcs4)",
			s.Schema))
	}
	return warns, nil
}

// Log writes one audit row to pcs_log. Empty schema/table/partition
// become NULL. Logging is best-effort at call sites; this returns the
// error so callers can decide.
func (s *Store) Log(ctx context.Context, msgType, proc, schema, table, partition, message string) error {
	_, err := s.DB.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (part_schema, part_table, part_partition, message_type, logging_proc, message) VALUES (?, ?, ?, ?, ?, ?)",
		db.QuoteQualified(s.Schema, "pcs_log")),
		nullable(schema), nullable(table), nullable(partition), msgType, proc, message)
	if err != nil {
		return fmt.Errorf("writing pcs_log: %w", err)
	}
	return nil
}

// Info logs an informational audit row.
func (s *Store) Info(ctx context.Context, proc, schema, table, partition, message string) error {
	return s.Log(ctx, "Info", proc, schema, table, partition, message)
}

// Error logs an error audit row.
func (s *Store) Error(ctx context.Context, proc, schema, table, partition, message string) error {
	return s.Log(ctx, "Error", proc, schema, table, partition, message)
}

func nullable(v string) sql.NullString {
	return sql.NullString{String: v, Valid: v != ""}
}
