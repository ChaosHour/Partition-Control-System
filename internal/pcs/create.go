package pcs

import (
	"context"
	"fmt"
	"time"

	"github.com/ChaosHour/Partition-Control-System/internal/db"
)

// CreateHorizonDays is how many days of future partitions pcs maintains,
// matching the legacy pcs_create.
const CreateHorizonDays = 30

// ExtendTable appends partitions to an already-partitioned table until
// coverage reaches today+CreateHorizonDays (port of pcs_create, which
// never actually ran from the event because of the guard-variable typo —
// review finding F1). Returns the number of partitions added.
func (s *Store) ExtendTable(ctx context.Context, cfg ConfigRow, info *TableInfo, today time.Time) (int, error) {
	parts, err := s.Partitions(ctx, cfg.Schema, cfg.Table)
	if err != nil {
		return 0, err
	}
	if len(parts) == 0 {
		return 0, fmt.Errorf("%s.%s is not partitioned; state should be Init", cfg.Schema, cfg.Table)
	}
	newest := parts[len(parts)-1]
	if newest.IsMaxValue {
		s.Error(ctx, "Create", cfg.Schema, cfg.Table, newest.Name,
			"table has a MAXVALUE partition; cannot add daily partitions")
		return 0, fmt.Errorf("%s.%s has a MAXVALUE partition", cfg.Schema, cfg.Table)
	}

	// First day needing a partition, derived from the newest boundary.
	var firstDay time.Time
	switch info.Scheme {
	case SchemeToDays:
		// pD is bounded by TO_DAYS(D+1), so FromDays(boundary) is the
		// next partition's name day.
		firstDay = FromDays(newest.Description)
	case SchemeUnixTimestamp:
		// pD is bounded by UNIX_TIMESTAMP at day D; next name is D+1.
		day, err := s.EpochDay(ctx, newest.Description)
		if err != nil {
			return 0, err
		}
		firstDay = day.AddDate(0, 0, 1)
	default:
		return 0, fmt.Errorf("%s.%s: unsupported scheme", cfg.Schema, cfg.Table)
	}

	existing := make(map[string]bool, len(parts))
	for _, p := range parts {
		existing[p.Name] = true
	}

	added := 0
	for _, spec := range PlanCreate(info.Scheme, firstDay, today, CreateHorizonDays, int(cfg.BoundaryHour)) {
		if existing[spec.Name] {
			s.Error(ctx, "Create", cfg.Schema, cfg.Table, spec.Name, "partition name already exists; skipping")
			continue
		}
		alter := fmt.Sprintf("ALTER TABLE %s ADD PARTITION (PARTITION %s VALUES LESS THAN (%s))",
			db.QuoteQualified(cfg.Schema, cfg.Table), spec.Name, spec.Boundary)
		if err := s.execDDL(ctx, alter); err != nil {
			msg := fmt.Sprintf("adding partition failed: %v", err)
			s.Error(ctx, "Create", cfg.Schema, cfg.Table, spec.Name, msg)
			return added, fmt.Errorf("adding partition %s to %s.%s: %w", spec.Name, cfg.Schema, cfg.Table, err)
		}
		s.Info(ctx, "Create", cfg.Schema, cfg.Table, spec.Name, "created with boundary "+spec.Boundary)
		added++
	}
	return added, nil
}
