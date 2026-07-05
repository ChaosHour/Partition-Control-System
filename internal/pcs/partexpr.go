package pcs

import "strings"

// Scheme is the partition-expression scheme PCS manages. The system
// creates daily RANGE partitions either on TO_DAYS(datetime_col) or
// UNIX_TIMESTAMP(timestamp_col); anything else is out of scope.
type Scheme int

const (
	SchemeUnknown Scheme = iota
	SchemeToDays
	SchemeUnixTimestamp
)

func (s Scheme) String() string {
	switch s {
	case SchemeToDays:
		return "TO_DAYS"
	case SchemeUnixTimestamp:
		return "UNIX_TIMESTAMP"
	default:
		return "unknown"
	}
}

// ParsePartitionExpression extracts the column name and scheme from an
// information_schema.partitions PARTITION_EXPRESSION value. Handles the
// formatting differences across server versions: with or without
// backticks, any case, e.g. "to_days(`created_at`)", "TO_DAYS(created)",
// "unix_timestamp(`ts`)", or a bare column "`id`" (scheme unknown).
func ParsePartitionExpression(expr string) (column string, scheme Scheme) {
	e := strings.TrimSpace(expr)
	inner := e
	if fn, rest, found := strings.Cut(e, "("); found {
		switch strings.ToLower(strings.TrimSpace(fn)) {
		case "to_days":
			scheme = SchemeToDays
		case "unix_timestamp":
			scheme = SchemeUnixTimestamp
		}
		inner = rest
		if in, _, ok := strings.Cut(rest, ")"); ok {
			inner = in
		}
	}
	column = strings.Trim(strings.TrimSpace(inner), "`'\"")
	return column, scheme
}
