package pcs

import (
	"fmt"
	"time"
)

// Partition naming and boundary planning.
//
// PCS creates one partition per day, named pYYYYMMDD. Boundary semantics
// preserve what the legacy pcs_create procedure produced, so the Go
// edition can extend tables the stored-procedure edition partitioned:
//
//   - TO_DAYS (datetime): partition pD VALUES LESS THAN (TO_DAYS(D+1)).
//     pD holds day D.
//   - UNIX_TIMESTAMP (timestamp): partition pD VALUES LESS THAN
//     (UNIX_TIMESTAMP('D <boundary-hour>:00:00')). pD holds the day
//     *before* D, from boundary hour to boundary hour. (The asymmetry is
//     legacy; renaming existing partitions is not worth the churn.)
//
// Boundaries are emitted as SQL constant expressions, not precomputed
// numbers, so the server performs every timezone-sensitive conversion
// (UNIX_TIMESTAMP depends on the server time zone, including DST).

// mysqlEpochDays is TO_DAYS('1970-01-01').
const mysqlEpochDays = 719528

// PartitionSpec is one partition to create: a name and the SQL constant
// expression for its VALUES LESS THAN clause.
type PartitionSpec struct {
	Name     string
	Boundary string
}

// ToDays returns MySQL TO_DAYS() for the date part of day.
func ToDays(day time.Time) int64 {
	d := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)
	return mysqlEpochDays + d.Unix()/86400
}

// FromDays is the inverse of ToDays, returning a UTC midnight date.
func FromDays(n int64) time.Time {
	return time.Unix((n-mysqlEpochDays)*86400, 0).UTC()
}

// PartitionName returns the pYYYYMMDD name for a day.
func PartitionName(day time.Time) string {
	return day.Format("p20060102")
}

// ParsePartitionName parses a pYYYYMMDD partition name.
func ParsePartitionName(name string) (time.Time, bool) {
	t, err := time.Parse("p20060102", name)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func boundaryExpr(scheme Scheme, day time.Time, boundaryHour int) string {
	switch scheme {
	case SchemeToDays:
		// pD holds day D: boundary is TO_DAYS of the following day.
		return fmt.Sprintf("TO_DAYS('%s')", day.AddDate(0, 0, 1).Format("2006-01-02"))
	case SchemeUnixTimestamp:
		// Legacy naming: pD's boundary is D itself at the boundary hour.
		return fmt.Sprintf("UNIX_TIMESTAMP('%s %02d:00:00')", day.Format("2006-01-02"), boundaryHour)
	}
	return ""
}

func planRange(scheme Scheme, firstDay, lastDay time.Time, boundaryHour int) []PartitionSpec {
	var specs []PartitionSpec
	for d := firstDay; !d.After(lastDay); d = d.AddDate(0, 0, 1) {
		specs = append(specs, PartitionSpec{
			Name:     PartitionName(d),
			Boundary: boundaryExpr(scheme, d, boundaryHour),
		})
	}
	return specs
}

// PlanInit returns the complete partition list for the initial
// PARTITION BY RANGE of an unpartitioned table: coverage from the oldest
// data day through tomorrow. The first RANGE partition also catches
// anything older than oldestDay.
func PlanInit(scheme Scheme, oldestDay, today time.Time, boundaryHour int) []PartitionSpec {
	tomorrow := today.AddDate(0, 0, 1)
	first := oldestDay
	if scheme == SchemeUnixTimestamp {
		// pD holds day D-1, so coverage of oldestDay starts at p(oldest+1).
		first = oldestDay.AddDate(0, 0, 1)
	}
	return planRange(scheme, first, tomorrow, boundaryHour)
}

// PlanCreate returns the partitions to append to an already-partitioned
// table: firstDay (the day after the current newest partition) through
// today+horizonDays. Empty when coverage is already sufficient.
func PlanCreate(scheme Scheme, firstDay, today time.Time, horizonDays, boundaryHour int) []PartitionSpec {
	return planRange(scheme, firstDay, today.AddDate(0, 0, horizonDays), boundaryHour)
}
