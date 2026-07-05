package pcs

import "testing"

func TestParsePartitionExpression(t *testing.T) {
	tests := []struct {
		expr   string
		column string
		scheme Scheme
	}{
		{"to_days(`created_at`)", "created_at", SchemeToDays},
		{"TO_DAYS(created_at)", "created_at", SchemeToDays},
		{" to_days( `created_at` ) ", "created_at", SchemeToDays},
		{"unix_timestamp(`ts`)", "ts", SchemeUnixTimestamp},
		{"UNIX_TIMESTAMP(ts)", "ts", SchemeUnixTimestamp},
		{"`id`", "id", SchemeUnknown},
		{"id", "id", SchemeUnknown},
		{"year(`d`)", "d", SchemeUnknown},
		{"YEAR(d)", "d", SchemeUnknown},
	}
	for _, tt := range tests {
		col, scheme := ParsePartitionExpression(tt.expr)
		if col != tt.column || scheme != tt.scheme {
			t.Errorf("ParsePartitionExpression(%q) = (%q, %v), want (%q, %v)",
				tt.expr, col, scheme, tt.column, tt.scheme)
		}
	}
}
