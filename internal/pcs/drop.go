package pcs

import (
	"context"
	"fmt"
	"time"

	"github.com/ChaosHour/Partition-Control-System/internal/db"
)

// dropSafetyDays is the history floor: partitions holding data newer
// than this many days back are never dropped, whatever keep_days says
// (the legacy "3 day history threshold").
const dropSafetyDays = 3

// EnforceRetention drops the oldest partitions of a table until at most
// keep_days+CreateHorizonDays remain (port of pcs_drop). keep_days 0
// means never drop. The safety brake is boundary-based: the legacy
// procedure only aborted when the oldest partition was named exactly
// p(today-3), which missed most dangerous cases. Returns the number of
// partitions dropped.
func (s *Store) EnforceRetention(ctx context.Context, cfg ConfigRow, info *TableInfo, today time.Time) (int, error) {
	if cfg.KeepDays == 0 {
		return 0, nil
	}
	// Defensive clamp for rows written outside pcs add (legacy data).
	keep := min(max(int(cfg.KeepDays), MinKeepDays), MaxKeepDays)

	parts, err := s.Partitions(ctx, cfg.Schema, cfg.Table)
	if err != nil {
		return 0, err
	}

	dropped := 0
	for len(parts)-dropped > keep+CreateHorizonDays {
		oldest := parts[dropped]

		// Newest day of data the partition can hold, from its boundary.
		var holdsThrough time.Time
		switch {
		case oldest.IsMaxValue:
			return dropped, fmt.Errorf("%s.%s: oldest partition is MAXVALUE; refusing to drop", cfg.Schema, cfg.Table)
		case info.Scheme == SchemeToDays:
			holdsThrough = FromDays(oldest.Description).AddDate(0, 0, -1)
		default:
			day, err := s.EpochDay(ctx, oldest.Description)
			if err != nil {
				return dropped, err
			}
			holdsThrough = day.AddDate(0, 0, -1)
		}
		if !holdsThrough.Before(today.AddDate(0, 0, -dropSafetyDays)) {
			s.Error(ctx, "Drop", cfg.Schema, cfg.Table, oldest.Name,
				fmt.Sprintf("%d day history threshold breached (partition holds data through %s); drop aborted",
					dropSafetyDays, holdsThrough.Format("2006-01-02")))
			break
		}

		alter := fmt.Sprintf("ALTER TABLE %s DROP PARTITION %s",
			db.QuoteQualified(cfg.Schema, cfg.Table), oldest.Name)
		if err := s.execDDL(ctx, alter); err != nil {
			msg := fmt.Sprintf("dropping partition failed: %v", err)
			s.Error(ctx, "Drop", cfg.Schema, cfg.Table, oldest.Name, msg)
			return dropped, fmt.Errorf("dropping partition %s of %s.%s: %w", oldest.Name, cfg.Schema, cfg.Table, err)
		}
		s.Info(ctx, "Drop", cfg.Schema, cfg.Table, oldest.Name, "dropped")
		dropped++
	}
	return dropped, nil
}
