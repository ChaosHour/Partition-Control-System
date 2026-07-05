# PCS Go Conversion Plan

Convert the Partition Control System (v3.0.0) from MySQL stored procedures /
events / triggers + Ansible deploy into a single Go binary, compatible with
MySQL 5.7 through MySQL 9.x (including AWS RDS/Aurora MySQL, GCP Cloud SQL
for MySQL, and Percona Server).

Branch: `feature/go-conversion`

**The original implementation stays.** `roles/`, `pcs.sql`, and the shell
checks remain in the repo untouched as the SQL/stored-procedure edition; the
Go binary is a parallel implementation, not a replacement. The two should
not *manage the same tables at the same time* (the MySQL event and a `pcs
run` cron doing concurrent DDL), but the review findings below stay valuable
as documentation of the original's behavior.

---

## 1. Code Review Findings (current system)

Issues found in `roles/files/pcs.sql` and the deploy code. These inform the
Go design — the port should fix all of them, not reproduce them.

### Critical

- **F1 — `@CALLING_PSC_PROC` vs `@CALLING_PCS_PROC` typo.** `run_pcs` sets
  `@CALLING_PCS_PROC='RUN'`, but `pcs_create` and `pcs_drop` guard on
  `@CALLING_PSC_PROC` (PSC vs PCS). The guard variable is always NULL, so
  both procedures log "This procedure cannot be called directly" and exit —
  **partition creates and drops never actually run** from the event.
- **F2 — Trigger enum mismatch.** The three `pcs_config` triggers insert
  `logging_proc = 'pcs_config_change'`, which is not a member of the
  `pcs_log.logging_proc` ENUM (`'Tables','Create','Drop','Main','Init',
  'Control_Table_Change','Event'`). Under strict SQL mode (the default since
  5.7) the trigger error **aborts every INSERT/UPDATE/DELETE on
  `pcs_config`** — the config table cannot be populated.
- **F3 — Replica detection broken on 8.0+.** The event checks
  `performance_schema.global_status` for `Slave_running`, a status variable
  **removed in MySQL 8.0**. On 8.0+ the query returns 0 rows, so every
  replica considers itself a writer. Also, `SHOW SLAVE STATUS` itself is
  removed in 8.4+ (`SHOW REPLICA STATUS` from 8.0.22). Needs
  version-aware handling.

### Bugs / correctness

- **F4 — `pcs_check` prints wrong dates for timestamp tables.** The summary
  message always uses `FROM_DAYS()` on the partition boundary, which is only
  valid for `TO_DAYS()`-partitioned (datetime) tables; for `UNIX_TIMESTAMP()`
  partitions it produces garbage.
- **F5 — `pcs_check` cursor handling.** `WHILE 1 DO` relies on an EXIT
  handler that closes the cursor, then falls through to a second
  `CLOSE cur_id` — error on the last row path.
- **F6 — `pcs_tables` re-declares `pcs_log`** with a *different* ENUM set
  (missing `'Event'`) than the top-of-file definition. Benign only because of
  `IF NOT EXISTS`; a fresh install via `pcs_tables` gets the wrong schema.
- **F7 — `keep_days` clamped only at drop time** (3–2048 in `pcs_drop`), not
  validated when rows are inserted via `pcs_config_insert`.
- **F8 — SQL injection surface.** Schema/table/column names are concatenated
  raw into DDL in every procedure. Anyone who can call `pcs_config_insert`
  can inject DDL fragments.

### Deprecations (5.7 → 9.x compatibility)

- **F9 — `CHARSET=utf8`**: deprecated alias for utf8mb3; use `utf8mb4`.
- **F10 — `int(10) unsigned`** display widths: deprecated in 8.0.17+.
- **F11 — Name collision**: trigger `pcs_config_insert` and procedure
  `pcs_config_insert` share a name — legal, but confusing; rename in port.

### Deploy (Ansible)

- **F12 — Deploy is stale/broken.** Tasks reference `partition_manager.sql`
  and `gds_pm.partition_manager_tables`, but the file was renamed `pcs.sql`
  and the schema is `pcs`. The role as committed cannot install v3.0.0.
- **F13 — Groupon-era paths** (`/var/groupon/percona/...`, check_mk
  scripts hardcoded to `gds_pm`) — all superseded by the Go binary.

---

## 2. Proposed Architecture (recommendation)

One statically-linked Go binary, `pcs`, that owns **all** logic. MySQL keeps
only two plain tables (`pcs.pcs_config`, `pcs.pcs_log`) for state and audit.
No stored procedures, no triggers, no MySQL EVENT.

Why move logic out of the server entirely:

- Version compatibility becomes a Go problem, not a SQL-dialect problem —
  one code path with small version-gated queries (5.7 vs 8.0 vs 8.4/9).
- Works identically on RDS/Aurora, GCP Cloud SQL, and on-prem. Neither RDS
  nor Cloud SQL grants SUPER, and Cloud SQL gates `event_scheduler` behind a
  database flag — irrelevant once there is no MySQL EVENT to schedule.
- The trigger/enum/guard-variable bug class (F1, F2, F6) disappears; logging
  is done by the binary in the same transaction scope.
- Testable: unit tests for partition math, integration tests via Docker
  against 5.7 / 8.0 / 8.4 / 9.x.

Scheduling: run `pcs run` from cron or a systemd timer (documented), with an
optional `pcs run --daemon --interval 24h` mode. The binary performs the
replica check itself (version-aware) and no-ops on replicas, preserving
current behavior.

### CLI surface (maps 1:1 to old procedures)

| Command | Replaces |
|---|---|
| `pcs install` | Ansible role + top of pcs.sql + `pcs_tables` (schema bootstrap + discover existing RANGE-partitioned tables) |
| `pcs add -schema S -table T -column C -keep-days N` | `pcs_config_insert` (+ validation, F7/F8 fixed) |
| `pcs run` | event + `run_pcs` + `pcs_init`/`pcs_create`/`pcs_drop`/`pcs_update_index` |
| `pcs check` | `pcs_check` + `check_db_partition_manager` (Nagios/check_mk exit codes) |
| `pcs status` / `pcs list` | ad-hoc SELECTs against pcs_config / pcs_log |
| `pcs version` | `pcs_version` |

### Behavior preserved from v3.0.0

- RANGE partitioning by `TO_DAYS(col)` for `datetime`, `UNIX_TIMESTAMP(col)`
  for `timestamp`; daily partitions named `pYYYYMMDD`.
- 30 days of future partitions; `keep_days` retention (0 = never drop,
  clamp 3–2048); 3-day-history safety abort on drop.
- Boundary-hour support for timestamp tables.
- Auto-add partition column to PRIMARY KEY at init (with warning — it
  rebuilds the table).
- `Init` / `Active` / `Ignore` table states; refuse tables with FKs;
  only timestamp/datetime columns.
- All actions logged to `pcs_log`.

### Stack

- Go 1.22+, `github.com/go-sql-driver/mysql`, stdlib `flag` with subcommands
  (or `cobra` — decide at scaffolding; my lean is stdlib to keep deps at ~1).
- Config via flags + env (`PCS_DSN`) + optional `~/.my.cnf`-style file. TLS
  supported (needed for RDS; MySQL 9 also drops `mysql_native_password`, so
  `caching_sha2_password` must work — the driver handles it).

### GCP Cloud SQL support

Two connection paths, both supported:

1. **Standard TCP DSN** — works out of the box with private IP, authorized
   public IP, or the Cloud SQL Auth Proxy running as a sidecar/systemd
   service. Zero extra code; just documentation.
2. **Built-in Cloud SQL Go connector** (`cloud.google.com/go/cloudsqlconn`) —
   `pcs --cloudsql-instance project:region:instance ...` dials the instance
   directly using Application Default Credentials, with automatic mTLS. Also
   enables `--iam-auth` for IAM database authentication (no stored DB
   password). No Auth Proxy process to babysit on the cron host.

Cloud SQL specifics already covered by the design:

- MySQL 5.7 / 8.0 / 8.4 are the available Cloud SQL versions — inside our
  5.7–9.x compatibility matrix.
- Cloud SQL read replicas are detected by the same version-aware
  `SHOW REPLICA STATUS` / `SHOW SLAVE STATUS` check (T2); `pcs run` no-ops
  on them.
- `performance_schema` is disabled by default on smaller Cloud SQL tiers —
  the Go design only touches `information_schema`, so this doesn't matter
  (the old event's F3 query would have broken here too).

---

## 3. Task List

Work one item at a time; check off as completed.

- [x] **T1. Scaffolding** — `go.mod`, `cmd/pcs/main.go`, subcommand routing,
      `internal/` layout, version constant, Makefile, `.gitignore`.
- [x] **T2. DB layer** — connection/DSN handling, server version detection
      (5.7/8.0/8.4/9.x), version-aware replica check
      (`SHOW REPLICA STATUS` vs `SHOW SLAVE STATUS`), identifier quoting
      helper (fixes F8). Verified live against 5.7.44 / 8.0.45 / 8.4.10 /
      9.7.1 in Docker, incl. read_only replica detection. Added `pcs ping`.
- [x] **T3. Schema bootstrap** (`pcs install`, part 1) — create `pcs` DB,
      `pcs_config`, `pcs_log` with utf8mb4, no display widths, consistent
      definitions (fixes F2/F6/F9/F10; triggers replaced by app-level audit
      logging). Refuses replicas; warns on legacy event/table layout;
      verified on 5.7/8.0/8.4/9 incl. coexistence with a live legacy
      install and `-schema pcs4` separation.
- [x] **T4. Discovery** (`pcs install`, part 2) — port `pcs_tables`: seed
      `pcs_config` from existing RANGE-partitioned tables via
      `information_schema.partitions`. Opt-in via `install -discover`;
      skips non-TO_DAYS/UNIX_TIMESTAMP schemes; verified on 5.7 and 9.
- [x] **T5. `pcs add`** — port `pcs_config_insert`: validate table/column
      exists, datatype is timestamp/datetime, **engine is InnoDB** (8.0+
      supports native InnoDB partitioning only; the old procs never checked
      engine), keep_days range (F7), insert or update config + log.
- [x] **T6. Partition math package** — pure functions: partition name/boundary
      generation for TO_DAYS and UNIX_TIMESTAMP schemes, boundary hour,
      30-day horizon. Boundaries emitted as SQL expressions so the server
      owns timezone conversion. Unit tests (no DB needed).
- [x] **T7. Init** — port `pcs_init` + `pcs_update_index`: PK check/rebuild,
      initial `PARTITION BY RANGE` ALTER from earliest data date; adopts
      already-partitioned tables with matching scheme.
- [x] **T8. Create** — port `pcs_create`: extend partitions 30 days out for
      both datatypes (fixes F1 — this code will actually run now).
- [x] **T9. Drop** — port `pcs_drop`: retention enforcement, boundary-based
      3-day safety abort, keep_days clamp.
- [x] **T10. `pcs run`** — port `run_pcs` + event logic: replica no-op check
      (fixes F3), per-table validation (exists, datatype, InnoDB, no FKs),
      state machine Init→Active, GET_LOCK single-run guard, `-dry-run`,
      structured logging to `pcs_log`. (Daemon mode dropped: cron is the
      scheduler.)
- [x] **T11. `pcs check`** — port `pcs_check` with correct date rendering per
      datatype (fixes F4/F5); Nagios-style exit codes (0/1/2) so it replaces
      `check_db_partition_manager` / check_mk wrappers.
- [x] **T12. `pcs status`** — config, table states, partition footprint,
      recent log entries.
- [x] **T13. Integration tests** — docker-compose matrix: MySQL 5.7, 8.0,
      8.4, 9.x; end-to-end: install → add (incl. rejections) → run →
      retention/brake → check degrade/repair → replica no-op
      (`make integration`).
- [x] **T14. Cloud SQL connector** — `-cloudsql-instance` / `-iam-auth`
      flags wiring `cloud.google.com/go/cloudsqlconn` into the DB layer;
      Auth Proxy / private-IP TCP path unchanged.
- [x] **T15. Docs** — rewrite README to describe both editions (original
      SQL/stored-proc edition and Go edition), cron/systemd examples
      (including Cloud Run jobs / Cloud Scheduler and EventBridge patterns
      for managed hosts), and a "don't run both editions against the same
      tables" note. Original `roles/` + `pcs.sql` remain untouched.

## 4. Decisions

1. **CLI framework**: stdlib `flag`. *(Decided: T1 shipped with stdlib.)*
2. **Original code**: `roles/`, `pcs.sql`, and shell checks stay in the repo
   permanently as the SQL/stored-proc edition. `pcs install` never touches
   an existing stored-proc install; if the `pcs_event` event is found and
   enabled, it warns that both editions should not manage the same tables.
   *(Decided per owner.)*
3. **Cloud SQL connector: built-in or proxy-only?** The connector pulls in
   Google Cloud dependencies (~a dozen transitive modules) but removes the
   Auth Proxy operational dependency and enables IAM auth.
   *Recommendation: build it in (T14) — it's isolated to the DB layer, and
   plain DSN users never touch that code path.*
4. **Commit style**: one commit per T-item on `feature/go-conversion`.

## 5. References

- [Percona: What is MySQL Partitioning?](https://www.percona.com/blog/what-is-mysql-partitioning/)
  — background on partition types and version changes. Points the Go port
  relies on:
  - PCS uses **RANGE** partitioning only (`TO_DAYS()` / `UNIX_TIMESTAMP()`
    expressions); LIST/HASH/KEY/COLUMNS are out of scope.
  - **MySQL 8.0 removed the generic partitioning storage-engine plugin**;
    partitioning is native to InnoDB (and NDB) only. Hence the InnoDB
    engine check in T5/T10 that the original procs lacked.
  - The **partition key must be part of every unique key** (incl. primary
    key) — this is why `pcs_update_index`/T7 rebuilds the PK to include the
    partition column.
  - `ALTER TABLE ... DROP PARTITION` is the whole point of PCS retention:
    metadata-speed deletes vs row-by-row `DELETE`.
  - Partition pruning only helps queries that filter on the partition
    column — worth restating in the README (T15).
- [MySQL 8.4/9.x removals](https://dev.mysql.com/doc/refman/8.4/en/replication-statements.html):
  `SHOW SLAVE STATUS` → `SHOW REPLICA STATUS` (T2), `mysql_native_password`
  removed in 9.0 (driver handles `caching_sha2_password`).
