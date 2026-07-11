package pcs

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/ChaosHour/Partition-Control-System/internal/db"
)

// PartitionStat is one partition in a TableReport. Rows/bytes come from
// information_schema statistics (estimates for InnoDB). ExactRows, Min
// and Max are filled only when the report is built with exact=true.
type PartitionStat struct {
	Name         string `json:"name"`
	Boundary     string `json:"boundary"`                // raw partition_description
	BoundaryTime string `json:"boundary_time,omitempty"` // boundary as a server date/time, when derivable
	RowsEstimate int64  `json:"rows_estimate"`
	DataBytes    int64  `json:"data_bytes"`
	IndexBytes   int64  `json:"index_bytes"`
	ExactRows    *int64 `json:"exact_rows,omitempty"`
	Min          string `json:"min,omitempty"`
	Max          string `json:"max,omitempty"`
}

// TableReport is the full partition inventory of one table, suitable for
// human display or JSON snapshots (e.g. before/after an upgrade).
type TableReport struct {
	Schema         string          `json:"schema"`
	Table          string          `json:"table"`
	Engine         string          `json:"engine"`
	Method         string          `json:"method"`
	Expression     string          `json:"expression,omitempty"`
	Column         string          `json:"column,omitempty"`
	Scheme         string          `json:"scheme,omitempty"`
	AutoIncrement  *int64          `json:"auto_increment,omitempty"`
	CreateTime     string          `json:"create_time,omitempty"`
	UpdateTime     string          `json:"update_time,omitempty"`
	PartitionCount int             `json:"partition_count"`
	RowsEstimate   int64           `json:"rows_estimate"`
	DataBytes      int64           `json:"data_bytes"`
	IndexBytes     int64           `json:"index_bytes"`
	Min            string          `json:"min,omitempty"` // MIN(partition column)
	Max            string          `json:"max,omitempty"` // MAX(partition column)
	Partitions     []PartitionStat `json:"partitions"`
	CreateTable    string          `json:"create_table"`
}

// ListPartitionedTables returns schema/table pairs of every partitioned
// table outside the system schemas and the PCS metadata schema, any
// partitioning method. database narrows to one database when non-empty.
func (s *Store) ListPartitionedTables(ctx context.Context, database string) ([][2]string, error) {
	q := `
		SELECT DISTINCT table_schema, table_name
		FROM information_schema.partitions
		WHERE partition_name IS NOT NULL
		  AND table_schema NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')
		  AND table_schema <> ?`
	args := []any{s.Schema}
	if database != "" {
		q += " AND table_schema = ?"
		args = append(args, database)
	}
	q += " ORDER BY table_schema, table_name"

	rows, err := s.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("listing partitioned tables: %w", err)
	}
	defer rows.Close()

	var out [][2]string
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return nil, err
		}
		out = append(out, [2]string{schema, table})
	}
	return out, rows.Err()
}

// Inspect builds the partition inventory of one table. With exact set it
// additionally runs COUNT(*)/MIN/MAX per partition — a scan of each
// partition, so expect table-sized work on large tables.
func (s *Store) Inspect(ctx context.Context, schema, table string, exact bool) (*TableReport, error) {
	r := &TableReport{Schema: schema, Table: table}

	rows, err := s.DB.QueryContext(ctx, `
		SELECT partition_name, partition_method,
		       COALESCE(partition_expression, ''), partition_description,
		       COALESCE(table_rows, 0), COALESCE(data_length, 0), COALESCE(index_length, 0)
		FROM information_schema.partitions
		WHERE table_schema = ? AND table_name = ? AND partition_name IS NOT NULL
		ORDER BY partition_ordinal_position`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("reading partitions of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var p PartitionStat
		var desc sql.NullString
		if err := rows.Scan(&p.Name, &r.Method, &r.Expression, &desc,
			&p.RowsEstimate, &p.DataBytes, &p.IndexBytes); err != nil {
			return nil, err
		}
		p.Boundary = desc.String
		r.Partitions = append(r.Partitions, p)
		r.RowsEstimate += p.RowsEstimate
		r.DataBytes += p.DataBytes
		r.IndexBytes += p.IndexBytes
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(r.Partitions) == 0 {
		return nil, fmt.Errorf("%s.%s is not partitioned (or does not exist)", schema, table)
	}
	r.PartitionCount = len(r.Partitions)

	if r.Expression != "" {
		col, scheme := ParsePartitionExpression(r.Expression)
		r.Column = col
		if scheme != SchemeUnknown {
			r.Scheme = scheme.String()
		}
	}

	var autoInc sql.NullInt64
	var createTime, updateTime sql.NullString
	err = s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(engine, ''), auto_increment, create_time, update_time
		FROM information_schema.tables
		WHERE table_schema = ? AND table_name = ?`, schema, table).
		Scan(&r.Engine, &autoInc, &createTime, &updateTime)
	if err != nil {
		return nil, fmt.Errorf("reading table metadata of %s.%s: %w", schema, table, err)
	}
	if autoInc.Valid {
		r.AutoIncrement = &autoInc.Int64
	}
	r.CreateTime, r.UpdateTime = createTime.String, updateTime.String

	var name string
	if err := s.DB.QueryRowContext(ctx,
		"SHOW CREATE TABLE "+db.QuoteQualified(schema, table)).Scan(&name, &r.CreateTable); err != nil {
		return nil, fmt.Errorf("SHOW CREATE TABLE %s.%s: %w", schema, table, err)
	}

	for i := range r.Partitions {
		if err := s.humanizeBoundary(ctx, r.Scheme, &r.Partitions[i]); err != nil {
			return nil, err
		}
	}

	if r.Column != "" {
		var mn, mx sql.NullString
		q := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s",
			db.QuoteIdent(r.Column), db.QuoteIdent(r.Column), db.QuoteQualified(schema, table))
		if err := s.DB.QueryRowContext(ctx, q).Scan(&mn, &mx); err != nil {
			return nil, fmt.Errorf("reading %s range of %s.%s: %w", r.Column, schema, table, err)
		}
		r.Min, r.Max = mn.String, mx.String
	}

	if exact {
		for i := range r.Partitions {
			if err := s.exactPartitionStats(ctx, schema, table, r.Column, &r.Partitions[i]); err != nil {
				return nil, err
			}
		}
	}
	return r, nil
}

// humanizeBoundary fills BoundaryTime for boundaries whose meaning is
// derivable: epoch seconds (UNIX_TIMESTAMP scheme, converted by the
// server so its timezone applies), TO_DAYS day numbers, and quoted
// temporal literals (RANGE COLUMNS), which are just unquoted.
func (s *Store) humanizeBoundary(ctx context.Context, scheme string, p *PartitionStat) error {
	if p.Boundary == "MAXVALUE" {
		return nil
	}
	if lit := strings.Trim(p.Boundary, "'"); lit != p.Boundary {
		p.BoundaryTime = lit
		return nil
	}
	n, err := strconv.ParseInt(p.Boundary, 10, 64)
	if err != nil {
		return nil // unparseable boundary: leave raw only
	}
	switch scheme {
	case SchemeToDays.String():
		p.BoundaryTime = FromDays(n).Format("2006-01-02")
	case SchemeUnixTimestamp.String():
		var t sql.NullString
		// CAST: a bare placeholder arrives as DECIMAL and FROM_UNIXTIME
		// then returns microsecond precision (".000000" noise).
		if err := s.DB.QueryRowContext(ctx,
			"SELECT FROM_UNIXTIME(CAST(? AS UNSIGNED))", n).Scan(&t); err != nil {
			return fmt.Errorf("converting boundary %d: %w", n, err)
		}
		p.BoundaryTime = t.String
	}
	return nil
}

// exactPartitionStats runs COUNT(*) (and MIN/MAX of the partition column
// when known) against one partition.
func (s *Store) exactPartitionStats(ctx context.Context, schema, table, column string, p *PartitionStat) error {
	qual := db.QuoteQualified(schema, table)
	part := db.QuoteIdent(p.Name)
	var count int64
	if column == "" {
		q := fmt.Sprintf("SELECT COUNT(*) FROM %s PARTITION (%s)", qual, part)
		if err := s.DB.QueryRowContext(ctx, q).Scan(&count); err != nil {
			return fmt.Errorf("counting partition %s of %s.%s: %w", p.Name, schema, table, err)
		}
		p.ExactRows = &count
		return nil
	}
	col := db.QuoteIdent(column)
	q := fmt.Sprintf("SELECT COUNT(*), MIN(%s), MAX(%s) FROM %s PARTITION (%s)", col, col, qual, part)
	var mn, mx sql.NullString
	if err := s.DB.QueryRowContext(ctx, q).Scan(&count, &mn, &mx); err != nil {
		return fmt.Errorf("counting partition %s of %s.%s: %w", p.Name, schema, table, err)
	}
	p.ExactRows = &count
	p.Min, p.Max = mn.String, mx.String
	return nil
}

// HumanBytes renders a byte count in binary units (KiB, MiB, ...).
func HumanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// Comma renders n with thousands separators (1234567 -> "1,234,567").
func Comma(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var b strings.Builder
	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}
