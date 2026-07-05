package db

import (
	"context"
	"fmt"
)

// IsReplica reports whether the server should be treated as a replica, in
// which case pcs run must no-op (partition DDL replicates from the
// source). The returned reason explains the determination for logging.
//
// The stored-procedure edition checked the Slave_running status variable,
// which was removed in MySQL 8.0 (review finding F3). Instead we use:
//
//  1. SHOW REPLICA STATUS (8.0.22+) / SHOW SLAVE STATUS (older; removed
//     in 8.4) — rows returned means replication is configured. Covers
//     classic replication, RDS and Cloud SQL read replicas.
//  2. @@global.read_only / @@global.innodb_read_only — covers Aurora
//     readers (which report no replica status) and manually frozen
//     writers; DDL would fail there anyway.
func (d *DB) IsReplica(ctx context.Context) (bool, string, error) {
	stmt := "SHOW SLAVE STATUS"
	if d.Version.AtLeast(8, 0, 22) {
		stmt = "SHOW REPLICA STATUS"
	}
	rows, err := d.QueryContext(ctx, stmt)
	if err == nil {
		configured := rows.Next()
		rerr := rows.Err()
		rows.Close()
		if rerr != nil {
			return false, "", fmt.Errorf("%s: %w", stmt, rerr)
		}
		if configured {
			return true, fmt.Sprintf("replication configured (%s returned a row)", stmt), nil
		}
	}
	// err != nil usually means the user lacks REPLICATION CLIENT; fall
	// through to the read_only checks rather than failing the run.

	var readOnly, innodbReadOnly int
	if err := d.QueryRowContext(ctx,
		"SELECT @@global.read_only, @@global.innodb_read_only").
		Scan(&readOnly, &innodbReadOnly); err != nil {
		return false, "", fmt.Errorf("checking read_only: %w", err)
	}
	if innodbReadOnly == 1 {
		return true, "innodb_read_only=ON (Aurora reader or read-only instance)", nil
	}
	if readOnly == 1 {
		return true, "read_only=ON", nil
	}
	return false, "writer (no replica status, read_only=OFF)", nil
}
