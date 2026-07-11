#!/usr/bin/env bash
# verify_84_precheck.sh — reproduce the deprecatedTemporalDelimiter evidence
# locally with Docker + mysqlsh. Three phases:
#   1. Source 8.0.28 (< 8.0.29): checker flags healthy UNIX_TIMESTAMP /
#      TO_DAYS partitions (false positives) + a genuinely bad table.
#   2. Source 8.0.x latest (>= 8.0.29): checker clean.
#   3. In-place upgrade of phase-2 datadir to 8.4: succeeds, table usable.
#
# Requires: docker, mysqlsh. Runs entirely in throwaway containers/volumes.
set -euo pipefail

PORT_OLD=33081 PORT_NEW=33080
C_OLD=pcs-ucheck28 C_NEW=pcs-ucheck C_84=pcs-ucheck84 VOL=pcs-ucheck-data
PASS="\033[32mPASS\033[0m" FAIL="\033[31mFAIL\033[0m"
rc=0

cleanup() {
  docker rm -f "$C_OLD" "$C_NEW" "$C_84" >/dev/null 2>&1 || true
  docker volume rm "$VOL" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

wait_mysql() { # container
  for _ in $(seq 1 90); do
    docker exec "$1" mysql -uroot -ptest -e "SELECT 1" >/dev/null 2>&1 && return 0
    sleep 2
  done
  echo "MySQL in $1 did not come up" >&2; return 1
}

# Tables: kw_unix is the common production pattern (TIMESTAMP +
# RANGE(UNIX_TIMESTAMP), integer bounds, hyphenated names); kw_todays is
# the canonical TO_DAYS pattern (and PCS's scheme).
CORE_SQL=$(cat <<'SQL'
CREATE DATABASE kwtest;
USE kwtest;
CREATE TABLE kw_unix (
  id BIGINT NOT NULL AUTO_INCREMENT,
  last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id, last_updated)
) ENGINE=InnoDB
PARTITION BY RANGE (UNIX_TIMESTAMP(last_updated)) (
  PARTITION `2026-04-01` VALUES LESS THAN (1775001600),
  PARTITION `2026-05-01` VALUES LESS THAN (1777593600),
  PARTITION `2033` VALUES LESS THAN MAXVALUE);
CREATE TABLE kw_todays (
  id BIGINT NOT NULL AUTO_INCREMENT,
  created DATETIME NOT NULL,
  PRIMARY KEY (id, created)
) ENGINE=InnoDB
PARTITION BY RANGE (TO_DAYS(created)) (
  PARTITION p20260401 VALUES LESS THAN (TO_DAYS('2026-04-02')),
  PARTITION p20260402 VALUES LESS THAN (TO_DAYS('2026-04-03')));
SQL
)

# Only creatable on <= 8.0.28: the true positive the check exists for.
BAD_SQL=$(cat <<'SQL'
USE kwtest;
CREATE TABLE kw_bad (
  id BIGINT NOT NULL AUTO_INCREMENT,
  d DATE NOT NULL,
  PRIMARY KEY (id, d)
) ENGINE=InnoDB
PARTITION BY RANGE COLUMNS (d) (
  PARTITION p1 VALUES LESS THAN ('2026_04_01'),
  PARTITION p2 VALUES LESS THAN ('2026_05_01'));
SQL
)

check() { # port -> writes full checker output to stdout
  mysqlsh "root:test@127.0.0.1:$1" -- util check-for-server-upgrade \
    --target-version=8.4 2>&1
}

echo "=== Phase 1: source 8.0.28 (< 8.0.29) — expect temporal-delimiter ERRORS"
docker run -d --name "$C_OLD" -e MYSQL_ROOT_PASSWORD=test \
  -p "$PORT_OLD":3306 mysql:8.0.28 --skip-log-bin >/dev/null
wait_mysql "$C_OLD"
docker exec -i "$C_OLD" mysql -uroot -ptest 2>/dev/null <<<"$CORE_SQL"
docker exec -i "$C_OLD" mysql -uroot -ptest 2>/dev/null <<<"$BAD_SQL"
out=$(check "$PORT_OLD")
echo "$out" | grep -A20 "deprecatedTemporalDelimiter" | sed -n '1,20p' || true
if echo "$out" | grep -q "kw_unix.last_updated" && \
   echo "$out" | grep -q "kw_todays.created"; then
  echo -e "$PASS: healthy integer-boundary tables falsely flagged on old source"
else
  echo -e "$FAIL: expected false positives not reported"; rc=1
fi

echo
echo "=== Phase 2: source 8.0 latest (>= 8.0.29) — expect CLEAN"
docker run -d --name "$C_NEW" -e MYSQL_ROOT_PASSWORD=test \
  -p "$PORT_NEW":3306 -v "$VOL":/var/lib/mysql mysql:8.0 --skip-log-bin >/dev/null
wait_mysql "$C_NEW"
docker exec -i "$C_NEW" mysql -uroot -ptest 2>/dev/null <<<"$CORE_SQL"
if docker exec -i "$C_NEW" mysql -uroot -ptest 2>/dev/null <<<"$BAD_SQL"; then
  echo -e "$FAIL: bad-delimiter table unexpectedly accepted on modern 8.0"; rc=1
else
  echo -e "$PASS: bad-delimiter literals rejected at CREATE on modern 8.0"
fi
out=$(check "$PORT_NEW")
errs=$(echo "$out" | awk '/^Errors:/{print $2}')
if [ "${errs:-1}" = "0" ] && ! echo "$out" | grep -q "temporal delimiters"; then
  echo -e "$PASS: checker reports 0 errors for the keywords pattern (errors=$errs)"
else
  echo -e "$FAIL: checker not clean (errors=$errs)"; rc=1
fi

echo
echo "=== Phase 3: in-place upgrade of that datadir to 8.4 — expect SUCCESS"
docker rm -f "$C_NEW" >/dev/null
docker run -d --name "$C_84" -e MYSQL_ROOT_PASSWORD=test \
  -p "$PORT_NEW":3306 -v "$VOL":/var/lib/mysql mysql:8.4 --skip-log-bin >/dev/null
wait_mysql "$C_84"
ver=$(docker exec "$C_84" mysql -uroot -ptest -N -e "SELECT VERSION()" 2>/dev/null)
docker exec "$C_84" mysql -uroot -ptest -e \
  "INSERT INTO kwtest.kw_unix (last_updated) VALUES ('2026-04-15 12:00:00');
   SELECT COUNT(*) FROM kwtest.kw_unix PARTITION (\`2026-05-01\`);" >/dev/null 2>&1 \
  && echo -e "$PASS: upgraded to $ver; hyphen-named UNIX_TIMESTAMP table readable+writable" \
  || { echo -e "$FAIL: table not usable after upgrade to $ver"; rc=1; }
docker logs "$C_84" 2>&1 | grep "Server upgrade from" | tail -2

echo
[ "$rc" = 0 ] && echo -e "$PASS: all phases confirmed — no partition conversion needed" \
              || echo -e "$FAIL: see phases above"
exit "$rc"
