package db

import "testing"

func TestParseVersion(t *testing.T) {
	tests := []struct {
		raw                 string
		major, minor, patch int
	}{
		{"5.7.44-log", 5, 7, 44},
		{"5.7.12-enterprise-commercial-advanced-log", 5, 7, 12},
		{"8.0.36-28", 8, 0, 36},              // Percona Server
		{"8.0.34-google", 8, 0, 34},          // Cloud SQL
		{"8.0.mysql_aurora.3.04.0", 8, 0, 0}, // Aurora MySQL 3
		{"8.4.5", 8, 4, 5},
		{"9.3.0", 9, 3, 0},
	}
	for _, tt := range tests {
		v := ParseVersion(tt.raw)
		if v.Major != tt.major || v.Minor != tt.minor || v.Patch != tt.patch {
			t.Errorf("ParseVersion(%q) = %d.%d.%d, want %d.%d.%d",
				tt.raw, v.Major, v.Minor, v.Patch, tt.major, tt.minor, tt.patch)
		}
	}
}

func TestAtLeast(t *testing.T) {
	tests := []struct {
		v                   string
		major, minor, patch int
		want                bool
	}{
		{"8.0.22", 8, 0, 22, true},
		{"8.0.21", 8, 0, 22, false},
		{"8.0.36-28", 8, 0, 22, true},
		{"5.7.44-log", 8, 0, 22, false},
		{"8.4.0", 8, 0, 22, true},
		{"9.0.0", 8, 0, 22, true},
		{"9.0.0", 8, 4, 0, true},
		{"8.0.mysql_aurora.3.04.0", 8, 0, 22, false}, // patch unknown: conservative
	}
	for _, tt := range tests {
		if got := ParseVersion(tt.v).AtLeast(tt.major, tt.minor, tt.patch); got != tt.want {
			t.Errorf("ParseVersion(%q).AtLeast(%d,%d,%d) = %v, want %v",
				tt.v, tt.major, tt.minor, tt.patch, got, tt.want)
		}
	}
}
