#!/usr/bin/env bash
# End-to-end integration suite for the pcs Go edition.
#
# Drives the built binary against the MySQL version matrix from
# test/docker-compose.yml (5.7, 8.0, 8.4, 9.x): install, add (including
# rejection paths), run (init/extend/retention/safety brake), check
# (healthy, degraded, repaired), replica no-op, idempotence.
#
# Usage: bash test/integration.sh            # all versions
#        bash test/integration.sh 34080      # one port only
set -u

BIN=${PCS_BIN:-./bin/pcs}
PASS=pcs-int
export PCS_PASSWORD=$PASS PCS_HOST=127.0.0.1

MATRIX=(
  "pcs-int-57 34057 5.7"
  "pcs-int-80 34080 8.0"
  "pcs-int-84 34084 8.4"
  "pcs-int-9  34090 9"
)

fails=0
oks=0
note() { printf '  ok   %s\n' "$1"; oks=$((oks + 1)); }
fail() { printf '  FAIL %s\n' "$1"; fails=$((fails + 1)); }
check_eq() { # description expected actual
  if [ "$2" = "$3" ]; then note "$1"; else fail "$1 (want '$2', got '$3')"; fi
}

sq() { # container: run SQL from arg, one result value
  docker exec "$1" mysql -uroot -p$PASS -N -B -e "$2" 2>/dev/null
}
sqm() { # container: run multi-statement SQL from stdin
  docker exec -i "$1" mysql -uroot -p$PASS 2>/dev/null
}

wait_ready() { # port
  for _ in $(seq 1 90); do
    "$BIN" ping -port "$1" >/dev/null 2>&1 && return 0
    sleep 2
  done
  return 1
}

suite() { # container port label
  local ctr=$1 port=$2 ver=$3 today plus30 out
  echo "== MySQL $ver (port $port) =="

  if ! wait_ready "$port"; then
    fail "$ver: server did not become ready"
    return
  fi

  # Fresh slate for reruns of the suite.
  sqm "$ctr" <<'SQL'
DROP DATABASE IF EXISTS pcs;
DROP DATABASE IF EXISTS intdb;
CREATE DATABASE intdb;
CREATE TABLE intdb.orders (
  id bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
  created_at datetime NOT NULL,
  amount decimal(10,2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO intdb.orders (created_at, amount)
SELECT NOW() - INTERVAL n DAY, n FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 5 UNION SELECT 10) d;
CREATE TABLE intdb.metrics (
  id bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  val double
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO intdb.metrics (ts, val) VALUES (NOW() - INTERVAL 2 DAY, 1), (NOW(), 2);
CREATE TABLE intdb.hist (
  id bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
  created_at datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO intdb.hist (created_at) VALUES (NOW() - INTERVAL 80 DAY), (NOW());
CREATE TABLE intdb.badengine (id int PRIMARY KEY, ts timestamp) ENGINE=MyISAM;
CREATE TABLE intdb.parent (id int PRIMARY KEY, ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;
CREATE TABLE intdb.child (id int PRIMARY KEY, pid int, ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (pid) REFERENCES intdb.parent(id)) ENGINE=InnoDB;
SQL
  today=$(sq "$ctr" "SELECT DATE_FORMAT(CURDATE(), '%Y%m%d')")
  plus30=$(sq "$ctr" "SELECT DATE_FORMAT(CURDATE() + INTERVAL 30 DAY, '%Y%m%d')")

  # --- install (idempotent) ---
  "$BIN" install -port "$port" >/dev/null 2>&1; check_eq "install" 0 $?
  "$BIN" install -port "$port" >/dev/null 2>&1; check_eq "install rerun" 0 $?

  # --- add: happy paths ---
  "$BIN" add -port "$port" -database intdb -table orders -column created_at -keep-days 40 >/dev/null 2>&1
  check_eq "add orders" 0 $?
  "$BIN" add -port "$port" -database intdb -table metrics -column ts -keep-days 35 -boundary-hour 5 >/dev/null 2>&1
  check_eq "add metrics (boundary hour 5)" 0 $?
  "$BIN" add -port "$port" -database intdb -table hist -column created_at -keep-days 3 >/dev/null 2>&1
  check_eq "add hist" 0 $?

  # --- add: rejections ---
  "$BIN" add -port "$port" -database intdb -table badengine -column ts >/dev/null 2>&1
  check_eq "add rejects MyISAM" 1 $?
  "$BIN" add -port "$port" -database intdb -table child -column ts >/dev/null 2>&1
  check_eq "add rejects FK table" 1 $?
  "$BIN" add -port "$port" -database intdb -table orders -column amount >/dev/null 2>&1
  check_eq "add rejects decimal column" 1 $?
  "$BIN" add -port "$port" -database intdb -table orders -column created_at -keep-days 2 >/dev/null 2>&1
  check_eq "add rejects keep-days 2" 1 $?

  # --- first run: init + extend ---
  "$BIN" run -port "$port" >/dev/null 2>&1; check_eq "run (init)" 0 $?
  check_eq "orders partition count" 41 \
    "$(sq "$ctr" "SELECT COUNT(*) FROM information_schema.partitions WHERE table_schema='intdb' AND table_name='orders' AND partition_name IS NOT NULL")"
  check_eq "orders newest partition" "p$plus30" \
    "$(sq "$ctr" "SELECT partition_name FROM information_schema.partitions WHERE table_schema='intdb' AND table_name='orders' AND partition_name IS NOT NULL ORDER BY partition_ordinal_position DESC LIMIT 1")"
  check_eq "orders PK includes created_at" 1 \
    "$(sq "$ctr" "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema='intdb' AND table_name='orders' AND index_name='PRIMARY' AND column_name='created_at'")"
  check_eq "orders rows all placed" 4 \
    "$(sq "$ctr" "SELECT COUNT(*) FROM intdb.orders")"
  check_eq "metrics boundary honors hour 5" \
    "$(sq "$ctr" "SELECT UNIX_TIMESTAMP(DATE_ADD(CURDATE(), INTERVAL 30 DAY) + INTERVAL 5 HOUR)")" \
    "$(sq "$ctr" "SELECT partition_description FROM information_schema.partitions WHERE table_schema='intdb' AND table_name='metrics' AND partition_name IS NOT NULL ORDER BY partition_ordinal_position DESC LIMIT 1")"
  check_eq "states now Active" 3 \
    "$(sq "$ctr" "SELECT COUNT(*) FROM pcs.pcs_config WHERE table_state='Active'")"

  # --- second run: idempotence + retention with safety brake ---
  "$BIN" run -port "$port" >/dev/null 2>&1; check_eq "run rerun" 0 $?
  check_eq "orders count unchanged" 41 \
    "$(sq "$ctr" "SELECT COUNT(*) FROM information_schema.partitions WHERE table_schema='intdb' AND table_name='orders' AND partition_name IS NOT NULL")"
  check_eq "hist retention (keep 3 + horizon 30 + brake)" 34 \
    "$(sq "$ctr" "SELECT COUNT(*) FROM information_schema.partitions WHERE table_schema='intdb' AND table_name='hist' AND partition_name IS NOT NULL")"
  check_eq "hist brake logged" 1 \
    "$(sq "$ctr" "SELECT COUNT(*) > 0 FROM pcs.pcs_log WHERE part_table='hist' AND logging_proc='Drop' AND message_type='Error'")"
  check_eq "hist recent data intact" 1 \
    "$(sq "$ctr" "SELECT COUNT(*) FROM intdb.hist")"

  # --- check: healthy -> degraded -> repaired ---
  "$BIN" check -port "$port" >/dev/null 2>&1; check_eq "check healthy" 0 $?
  sq "$ctr" "SET @s=(SELECT CONCAT('ALTER TABLE intdb.orders DROP PARTITION ', GROUP_CONCAT(partition_name)) FROM information_schema.partitions WHERE table_schema='intdb' AND table_name='orders' AND partition_name > 'p$today'); PREPARE p FROM @s; EXECUTE p" >/dev/null
  "$BIN" check -port "$port" >/dev/null 2>&1; check_eq "check detects lost coverage" 2 $?
  "$BIN" run -port "$port" >/dev/null 2>&1
  "$BIN" check -port "$port" >/dev/null 2>&1; check_eq "check ok after repair run" 0 $?

  # --- replica no-op ---
  sq "$ctr" "SET GLOBAL read_only=1" >/dev/null
  out=$("$BIN" run -port "$port" 2>&1); rc=$?
  sq "$ctr" "SET GLOBAL read_only=0" >/dev/null
  check_eq "run on read_only exits 0" 0 $rc
  case "$out" in *replica*) note "run on read_only skips as replica" ;; *) fail "run on read_only skips as replica (got: $out)" ;; esac
}

only=${1:-}
for entry in "${MATRIX[@]}"; do
  set -- $entry
  ctr=$1 port=$2 ver=$3
  if [ -n "$only" ] && [ "$port" != "$only" ]; then continue; fi
  suite "$ctr" "$port" "$ver"
done

echo
echo "integration: $oks passed, $fails failed"
[ "$fails" -eq 0 ]
