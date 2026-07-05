# Partition Control System (PCS)

Automated daily RANGE partition management for MySQL: creates tomorrow's
partitions ahead of time, drops expired ones, and monitors that both keep
happening. Tables are partitioned by day on a `datetime` column
(`TO_DAYS`) or a `timestamp` column (`UNIX_TIMESTAMP`), giving
metadata-speed retention (`DROP PARTITION`) instead of row-by-row
`DELETE`s.

Two editions live in this repository:

| | Go edition (`pcs`) | SQL edition (original) |
|---|---|---|
| Location | `cmd/`, `internal/` | `roles/files/pcs.sql` + Ansible role |
| Logic runs | in a single static binary, scheduled by cron/systemd/Cloud Scheduler | in stored procedures, driven by a MySQL EVENT |
| MySQL support | 5.7, 8.0, 8.4, 9.x; Percona Server; AWS RDS/Aurora; GCP Cloud SQL | 5.7-era (replica detection breaks on 8.0+) |
| Status | active development | preserved as-is for reference/existing installs |

**Do not let both editions manage the same tables.** If the legacy
`pcs.pcs_event` is enabled, disable it before scheduling `pcs run`
(`ALTER EVENT pcs.pcs_event DISABLE`); `pcs install` warns about this.

## Install

```console
$ make build          # bin/pcs (or: make linux for a static linux/amd64 binary)
$ pcs install -host db1 -user root        # creates the pcs metadata schema
$ pcs install -host db1 -discover         # optionally adopt already-partitioned tables
```

Credentials come from flags or `PCS_HOST` / `PCS_USER` / `PCS_PASSWORD` /
`PCS_DSN` environment variables. Add `-tls true` for RDS/Cloud SQL public
endpoints. MySQL 9 works out of the box (`caching_sha2_password`).

The MySQL account needs: `ALTER`, `SELECT`, `INSERT`, `UPDATE`, `CREATE`,
`DROP` on the managed schemas and the `pcs` schema, plus
`REPLICATION CLIENT` (optional; improves replica detection).

## Register tables

```console
$ pcs add -host db1 -database shop -table orders  -column created_at -keep-days 90
$ pcs add -host db1 -database shop -table metrics -column ts -keep-days 30 -boundary-hour 5
```

Validation happens up front: the column must be `datetime` or
`timestamp`, the engine InnoDB, no foreign keys in either direction, and
`keep-days` must be 0 (never drop) or 3–2048. New tables start in state
`Init` and get partitioned on the next run; the partition column is added
to the primary key automatically if missing (**note: that is a full table
rebuild on a big table**).

## Run daily

```console
$ pcs run -host db1 -dry-run   # print the DDL a run would execute
$ pcs run -host db1            # do it
```

Per run, for every registered table: `Init` tables get their initial
`PARTITION BY RANGE` from the oldest data day; `Active` tables get
retention enforced (never touching partitions holding data from the last
3 days) and coverage extended to today+30. Replicas are detected
(including Aurora readers) and skipped — DDL replicates from the source.
Everything is audited in `pcs.pcs_log`.

Schedule it once a day on the writer, e.g.:

```
# cron
7 0 * * * PCS_PASSWORD=... /usr/local/bin/pcs run -host db1 -user pcs >> /var/log/pcs.log 2>&1
```

```ini
# systemd timer (pcs-run.timer -> pcs-run.service)
[Service]
Type=oneshot
EnvironmentFile=/etc/pcs/env
ExecStart=/usr/local/bin/pcs run -host db1 -user pcs
```

For managed clouds, any small scheduled compute works: GCP Cloud
Scheduler → Cloud Run job, AWS EventBridge → ECS/Lambda-with-container,
or plain cron on a bastion. The binary is static; the container image can
be `FROM scratch`.

## Monitor

```console
$ pcs check -host db1
OK   shop.orders: 121 partitions, coverage through 2026-08-04 (TO_DAYS)
$ echo $?
0
```

Nagios-style exit codes: `0` healthy, `1` warning (Init awaiting run,
retention anomalies, MAXVALUE partition), `2` critical (coverage not
extending — the daily run is broken, act before partitions run out).
`pcs status` shows the config, partition footprint and recent audit log.

## GCP Cloud SQL

Two options:

1. **Plain TCP** — private IP, authorized networks, or a Cloud SQL Auth
   Proxy sidecar: use `-host`/`-port` as usual.
2. **Built-in connector** — no proxy process, mTLS automatic, using
   Application Default Credentials:

```console
$ pcs run -cloudsql-instance my-project:us-central1:prod-db -user pcs
$ pcs run -cloudsql-instance my-project:us-central1:prod-db -user pcs-sa -iam-auth   # IAM database auth, no DB password
```

## Compatibility notes (5.7 → 9.x)

- Replica detection uses `SHOW REPLICA STATUS` on 8.0.22+ and
  `SHOW SLAVE STATUS` before that (removed in 8.4), with
  `read_only`/`innodb_read_only` fallback for Aurora readers.
- Partitioning is native-InnoDB-only since 8.0 (the generic partitioning
  engine was removed); `pcs add` enforces InnoDB.
- Partition boundaries are sent as SQL expressions
  (`TO_DAYS('...')`/`UNIX_TIMESTAMP('...')`) so the server's own time
  zone (incl. DST) governs them.
- Metadata tables use `utf8mb4` and no deprecated display widths.

## Development

```console
$ make test           # unit tests
$ make integration    # full matrix: MySQL 5.7/8.0/8.4/9 in Docker, end to end
```

`CONVERSION_PLAN.md` documents the port from the stored-procedure
edition, including the review findings it fixes (broken create/drop
guard, strict-mode trigger failures, 8.0+ replica detection, injection
surface, and more).

## History

PCS 3.0.0 (2022) was the stored-procedure rewrite of the original
Partition Manager (2010), created by David E. Minor with thanks to
Adrian Whitwham and Alex Gentile. The Go edition (4.x) is a from-scratch
port of the same behavior. MIT licensed.
