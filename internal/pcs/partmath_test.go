package pcs

import (
	"testing"
	"time"
)

func date(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// Reference values taken from a live MySQL 9 server.
func TestToDays(t *testing.T) {
	tests := []struct {
		day  time.Time
		want int64
	}{
		{date(1970, 1, 1), 719528},
		{date(2000, 1, 1), 730485},
		{date(2026, 7, 5), 740167},
	}
	for _, tt := range tests {
		if got := ToDays(tt.day); got != tt.want {
			t.Errorf("ToDays(%s) = %d, want %d", tt.day.Format("2006-01-02"), got, tt.want)
		}
		if back := FromDays(tt.want); !back.Equal(tt.day) {
			t.Errorf("FromDays(%d) = %s, want %s", tt.want, back, tt.day)
		}
	}
}

func TestPartitionName(t *testing.T) {
	d := date(2026, 7, 5)
	if got := PartitionName(d); got != "p20260705" {
		t.Errorf("PartitionName = %q", got)
	}
	back, ok := ParsePartitionName("p20260705")
	if !ok || !back.Equal(d) {
		t.Errorf("ParsePartitionName roundtrip failed: %v %v", back, ok)
	}
	if _, ok := ParsePartitionName("pmax"); ok {
		t.Error("ParsePartitionName accepted pmax")
	}
	if _, ok := ParsePartitionName("p2026075"); ok {
		t.Error("ParsePartitionName accepted short name")
	}
}

func TestPlanInitToDays(t *testing.T) {
	// Data back to 2026-07-01, today 2026-07-05: p20260701..p20260706,
	// each pD bounded by TO_DAYS(D+1) so pD holds day D.
	specs := PlanInit(SchemeToDays, date(2026, 7, 1), date(2026, 7, 5), 0)
	if len(specs) != 6 {
		t.Fatalf("got %d specs, want 6", len(specs))
	}
	if specs[0].Name != "p20260701" || specs[0].Boundary != "TO_DAYS('2026-07-02')" {
		t.Errorf("first = %+v", specs[0])
	}
	if specs[5].Name != "p20260706" || specs[5].Boundary != "TO_DAYS('2026-07-07')" {
		t.Errorf("last = %+v", specs[5])
	}
}

func TestPlanInitUnix(t *testing.T) {
	// Unix scheme with legacy naming: pD holds day D-1, so coverage of
	// 2026-07-01 begins at p20260702; boundary hour 5.
	specs := PlanInit(SchemeUnixTimestamp, date(2026, 7, 1), date(2026, 7, 5), 5)
	if len(specs) != 5 {
		t.Fatalf("got %d specs, want 5", len(specs))
	}
	if specs[0].Name != "p20260702" || specs[0].Boundary != "UNIX_TIMESTAMP('2026-07-02 05:00:00')" {
		t.Errorf("first = %+v", specs[0])
	}
	if specs[4].Name != "p20260706" || specs[4].Boundary != "UNIX_TIMESTAMP('2026-07-06 05:00:00')" {
		t.Errorf("last = %+v", specs[4])
	}
}

func TestPlanCreate(t *testing.T) {
	// Newest partition covers through 2026-07-06; horizon 30 days from
	// today 2026-07-05 → p20260707..p20260804 (29 partitions).
	specs := PlanCreate(SchemeToDays, date(2026, 7, 7), date(2026, 7, 5), 30, 0)
	if len(specs) != 29 {
		t.Fatalf("got %d specs, want 29", len(specs))
	}
	if specs[0].Name != "p20260707" {
		t.Errorf("first = %+v", specs[0])
	}
	if specs[28].Name != "p20260804" || specs[28].Boundary != "TO_DAYS('2026-08-05')" {
		t.Errorf("last = %+v", specs[28])
	}

	// Already covered: firstDay beyond horizon → no work.
	if got := PlanCreate(SchemeToDays, date(2026, 9, 1), date(2026, 7, 5), 30, 0); len(got) != 0 {
		t.Errorf("expected no specs, got %d", len(got))
	}
}
