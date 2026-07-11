# MySQL 8.0 → 8.4: the "deprecated temporal delimiters" pre-check false positive

Upgrading MySQL 8.0 to 8.4 (Cloud SQL in-place upgrade, or a manual
upgrade preceded by MySQL Shell's `util.checkForServerUpgrade`) can fail
its pre-check with a wall of errors like:

```
ERROR: 123 errors were found. Please correct these issues before upgrading

  mydb.events.last_updated -  - partition 2032-12-01 uses
     deprecated temporal delimiters
```

against tables partitioned the canonical way:

```sql
PARTITION BY RANGE (UNIX_TIMESTAMP(ts_col)) (...)   -- or TO_DAYS(dt_col)
```

**Those tables are healthy. Do not rebuild, rename or re-partition them.**
This document explains the check, when it misfires, and the actual fix.
`scripts/verify_84_precheck.sh` reproduces all of it locally with Docker.

## The real hazard the check exists for

MySQL 8.0.29 deprecated non-standard delimiters in *temporal literals*
used as partition boundaries — `RANGE COLUMNS(d)` with
`VALUES LESS THAN ('2022_02_01')` (underscores instead of hyphens). Such
tables are only creatable on ≤ 8.0.28, and after any upgrade crossing
8.0.29 they genuinely become inaccessible
(`ERROR 1654: Partition column values of incorrect type`) and can no
longer be repaired — that's
[Bug #113050](https://bugs.mysql.com/bug.php?id=113050). MySQL Shell
8.4.0 added the `deprecatedTemporalDelimiter` check for it.

## Why healthy tables get flagged

The check's SQL
([upgrade_check_creators.cc](https://github.com/mysql/mysql-shell/blob/master/modules/util/upgrade_checker/upgrade_check_creators.cc))
is a *negative* match: it flags any partition on a temporal column whose
`partition_description` does **not** look like a clean quoted literal
(`'YYYY-MM-DD …'`). Function-based partitioning stores **bare integers**
as boundaries (`1775001600` for `UNIX_TIMESTAMP`, day numbers for
`TO_DAYS`) — which can never match the regex, so *every* partition of
every such table is flagged. Both schemes PCS creates are affected. The
partition name in the message is cosmetic; names (hyphenated or not)
play no role, and renaming does not clear the check.

Two version gates decide who actually sees this:

- The check is registered to run only when the upgrade **crosses
  8.0.29** — i.e. the *source* server is older than 8.0.29
  ([upgrade_check_registry.cc](https://github.com/mysql/mysql-shell/blob/master/modules/util/upgrade_checker/upgrade_check_registry.cc)).
- On sources ≥ 8.0.29 the server rejects bad literals at `CREATE`, so
  the real hazard cannot exist there and the check is skipped.

So the error appears exactly when a pre-8.0.29 instance is checked
against an 8.4 target — and every function-partitioned table on it turns
into pre-check errors.

Verified empirically (2026-07, `scripts/verify_84_precheck.sh`):

| Scenario | Result |
|---|---|
| Checker vs 8.0.28 source | ERRORS: healthy `UNIX_TIMESTAMP` and `TO_DAYS` tables flagged (false positives) plus a genuine `'2026_04_01'`-literal table (true positive) |
| Checker vs 8.0.46 source | 0 errors, same table structures (clean even with `--include=deprecatedTemporalDelimiter`) |
| `CREATE` with `'2026_04_01'` literal on 8.0.46 | rejected (`ERROR 1654`) — true positives can only pre-date 8.0.29 |
| In-place upgrade 8.0.46 → 8.4.10 | succeeds; `UNIX_TIMESTAMP`-partitioned table (hyphenated partition names included) fully readable and writable |

## The fix

1. **Audit for true positives first** — this is the one step that must
   happen *before* crossing 8.0.29, because genuinely bad tables can't
   be repaired afterwards:

   ```sql
   SELECT p.table_schema, p.table_name, p.partition_name, p.partition_description
   FROM information_schema.partitions p
   WHERE p.partition_name IS NOT NULL
     AND p.partition_description LIKE "'%'"
     AND p.partition_description NOT REGEXP
     "^'[0-9]{4}-[0-9]{2}-[0-9]{2}( [0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]*)?)?'";
   ```

   Empty result (the normal case): nothing on the instance is at risk.
   Any rows: `REORGANIZE PARTITION` each flagged partition into the
   identical boundary written with standard `-` delimiters, while still
   on the old minor.

2. **Update the instance to a current 8.0.x minor (≥ 8.0.29), then
   upgrade to 8.4.** On Cloud SQL the minor hop is *self-service
   maintenance* — a separate flag from the major-version picker (which
   only offers `MYSQL_8_0`/`MYSQL_8_4`):

   ```bash
   # what's available (full string, e.g. MYSQL_8_0_45.R20250304.00_01)
   gcloud --project=PROJECT sql instances describe INSTANCE \
     --format="value(availableMaintenanceVersions)"

   # apply it (maintenance restart; replicas update first automatically)
   gcloud --project=PROJECT sql instances patch INSTANCE \
     --maintenance-version=MAINTENANCE_VERSION

   # re-run the pre-check: the temporal-delimiter errors are gone
   gcloud --project=PROJECT beta sql instances pre-check-major-version-upgrade INSTANCE \
     --target-database-version=MYSQL_8_4

   # the major hop
   gcloud --project=PROJECT sql instances patch INSTANCE \
     --database-version=MYSQL_8_4 --include-replicas-for-major-version-upgrade
   ```

   Rehearse both hops on a clone
   (`gcloud sql instances clone INSTANCE INSTANCE-rehearsal`) before
   touching production.

3. **Validate partitions and data around each hop** with the PCS CLI:

   ```bash
   pcs info -host HOST -exact -json > before.json
   # ... upgrade ...
   pcs info -host HOST -exact -json > after.json
   diff before.json after.json
   ```

   `pcs info` snapshots partition layout, boundaries, sizes, exact row
   counts, MIN/MAX of the partition column and `SHOW CREATE TABLE` for
   every partitioned table — an empty diff (modulo timestamps) means
   partitions and data came through intact.

## References

- [Bug #113050 — tables inaccessible after upgrade with non-standard temporal delimiters](https://bugs.mysql.com/bug.php?id=113050)
- [MySQL Shell Upgrade Checker Utility](https://dev.mysql.com/doc/mysql-shell/8.4/en/mysql-shell-utilities-upgrade.html)
- [MySQL 8.0.29 release notes — temporal delimiter deprecation](https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-29.html)
- [Cloud SQL: in-place major version upgrade](https://docs.cloud.google.com/sql/docs/mysql/upgrade-major-db-version-inplace)
- [Cloud SQL: self-service maintenance](https://docs.cloud.google.com/sql/docs/mysql/self-service-maintenance)
