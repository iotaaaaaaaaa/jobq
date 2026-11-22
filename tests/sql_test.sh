#!/usr/bin/env bash
# Runs the queue's SQL files against a real Postgres using only psql, so the
# queries can be verified without the Go driver. Needs psql 16 or newer
# (for \bind) and DATABASE_URL pointing at a scratch database.
#
#   DATABASE_URL=postgres://postgres@localhost:5432/jobq_test ./tests/sql_test.sh
set -euo pipefail
: "${DATABASE_URL:?set DATABASE_URL to a scratch Postgres database}"
SQL="$(cd "$(dirname "$0")/.." && pwd)/internal/queue/sql"
export PGOPTIONS="-c client_min_messages=warning"
PSQL=(psql "$DATABASE_URL" -X -q -At -v ON_ERROR_STOP=1)

pass=0
ok()   { pass=$((pass + 1)); echo "  ok   $1"; }
check_eq() { if [ "$2" = "$3" ]; then ok "$1"; else echo "  FAIL $1"; echo "    got:  $2"; echo "    want: $3"; exit 1; fi; }

# run <file.sql> <params...>: executes one query file with bound parameters.
run() {
  local file="$1"; shift
  local binds=""
  for a in "$@"; do binds+=" '${a//\'/\'\'}'"; done
  { cat "$SQL/$file"; printf '\n\\bind%s \\g\n' "$binds"; } | "${PSQL[@]}"
}
sql() { "${PSQL[@]}" -c "$1"; }
reset() { sql "DROP TABLE IF EXISTS jobs" ; "${PSQL[@]}" -f "$SQL/schema.sql"; }

echo "schema"
reset
"${PSQL[@]}" -f "$SQL/schema.sql" && ok "schema can be applied twice"

echo "enqueue"
row="$(run enqueue.sql default email '{"to":"a"}' 0 '' 5 order-1)"
check_eq "new job is created" "$(echo "$row" | awk -F'|' '{print $1 "," $5 "," $NF}')" "1,queued,t"
row="$(run enqueue.sql default email '{}' 0 '' 5 order-1)"
check_eq "same idempotency key returns the first job" "$(echo "$row" | awk -F'|' '{print $1 "," $NF}')" "1,f"
row="$(run enqueue.sql other email '{}' 0 '' 5 order-1)"
check_eq "key is scoped to the queue" "$(echo "$row" | awk -F'|' '{print $NF}')" "t"
run enqueue.sql default a '{}' 0 '' 5 '' >/dev/null
run enqueue.sql default a '{}' 0 '' 5 '' >/dev/null
check_eq "jobs without a key are never merged" "$(sql "SELECT count(*) FROM jobs WHERE queue='default'")" "3"

echo "dequeue order and claiming"
reset
run enqueue.sql default low  '{}' 0  '' 5 '' >/dev/null
run enqueue.sql default high '{}' 10 '' 5 '' >/dev/null
run enqueue.sql default later '{}' 99 "$(date -u -d '+1 hour' +%FT%TZ)" 5 '' >/dev/null
got="$(run dequeue.sql default w1 10 60 | awk -F'|' '{print $3 ":" $5 ":" $8 ":" $11}' | sort | paste -sd' ')"
check_eq "due jobs claimed, future job left alone" "$got" "high:running:1:w1 low:running:1:w1"
check_eq "claimed jobs are not handed out again" "$(run dequeue.sql default w2 10 60 | wc -l | tr -d ' ')" "0"
check_eq "highest priority first with limit 1" \
  "$(reset; run enqueue.sql default low '{}' 0 '' 5 '' >/dev/null; run enqueue.sql default high '{}' 10 '' 5 '' >/dev/null; run dequeue.sql default w 1 60 | cut -d'|' -f3)" "high"

echo "complete, fail and fencing"
reset
run enqueue.sql default a '{}' 0 '' 2 '' >/dev/null
run dequeue.sql default w1 1 60 >/dev/null
run complete.sql 1 w2 1 >/dev/null
check_eq "another worker cannot complete the job" "$(sql "SELECT status FROM jobs WHERE id=1")" "running"
run complete.sql 1 w1 9 >/dev/null
check_eq "a wrong attempt number cannot complete the job" "$(sql "SELECT status FROM jobs WHERE id=1")" "running"
check_eq "first failure requeues with a delay" "$(run fail.sql 1 w1 1 boom 0.3)" "queued"
check_eq "not ready during the backoff delay" "$(run dequeue.sql default w1 1 60 | wc -l | tr -d ' ')" "0"
sleep 0.4
check_eq "ready after the delay, attempt 2" "$(run dequeue.sql default w1 1 60 | cut -d'|' -f8)" "2"
check_eq "failure on the last attempt goes dead" "$(run fail.sql 1 w1 2 'boom again' 0.1)" "dead"
check_eq "dead job keeps the error" "$(sql "SELECT last_error || ',' || (finished_at IS NOT NULL) FROM jobs WHERE id=1")" "boom again,true"
run retry.sql 1 >/dev/null
check_eq "manual retry resets the job" "$(sql "SELECT status || ',' || attempts FROM jobs WHERE id=1")" "queued,0"
run dequeue.sql default w1 1 60 >/dev/null
run complete.sql 1 w1 1 >/dev/null
check_eq "the holder completes it" "$(sql "SELECT status FROM jobs WHERE id=1")" "succeeded"

echo "leases"
reset
run enqueue.sql default a '{}' 0 '' 5 '' >/dev/null
run dequeue.sql default crashed 1 0.3 >/dev/null
check_eq "not taken while the lease is valid" "$(run dequeue.sql default other 1 60 | wc -l | tr -d ' ')" "0"
run extend.sql 1 crashed 1 0.6 >/dev/null
sleep 0.4
check_eq "an extended lease still protects the job" "$(run dequeue.sql default other 1 60 | wc -l | tr -d ' ')" "0"
sleep 0.4
check_eq "taken over after the lease expires" "$(run dequeue.sql default other 1 60 | awk -F'|' '{print $8 ":" $11}')" "2:other"
run complete.sql 1 crashed 1 >/dev/null
check_eq "the crashed worker's late result is ignored" "$(sql "SELECT status || ',' || locked_by FROM jobs WHERE id=1")" "running,other"

reset
run enqueue.sql default a '{}' 0 '' 1 '' >/dev/null
run dequeue.sql default crashed 1 0.2 >/dev/null
sleep 0.3
run reap.sql default >/dev/null
check_eq "crash on the final attempt is reaped as dead" "$(sql "SELECT status FROM jobs WHERE id=1")" "dead"
check_eq "and is not run again" "$(run dequeue.sql default other 1 60 | wc -l | tr -d ' ')" "0"

echo "cancel"
reset
run enqueue.sql default a '{}' 0 '' 5 '' >/dev/null
run cancel.sql 1 >/dev/null
check_eq "queued job cancelled" "$(sql "SELECT status FROM jobs WHERE id=1")" "cancelled"
check_eq "cancelled job is never dequeued" "$(run dequeue.sql default w 1 60 | wc -l | tr -d ' ')" "0"

echo "concurrency: 8 workers drain 2000 jobs"
reset
sql "INSERT INTO jobs (queue, type) SELECT 'default', 'work' FROM generate_series(1, 2000)" >/dev/null
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
for w in $(seq 8); do
  (
    while :; do
      out="$(run dequeue.sql default "w$w" 25 60 | cut -d'|' -f1)"
      [ -z "$out" ] && break
      echo "$out" >> "$TMP/w$w"
    done
  ) &
done
wait
check_eq "every job handed out" "$(cat "$TMP"/w* | wc -l | tr -d ' ')" "2000"
check_eq "no job handed out twice" "$(cat "$TMP"/w* | sort -u | wc -l | tr -d ' ')" "2000"
check_eq "more than one worker got work" "$([ "$(ls "$TMP" | wc -l)" -gt 1 ] && echo yes)" "yes"
check_eq "the database agrees" "$(sql "SELECT count(*) FROM jobs WHERE status='running' AND attempts=1")" "2000"

echo "query plan"
plan="$("${PSQL[@]}" -c "SET enable_seqscan=off; EXPLAIN SELECT id FROM jobs WHERE queue='default' AND status='queued' AND run_at <= now() ORDER BY priority DESC, run_at, id LIMIT 10")"
echo "$plan" | grep -q jobs_ready_idx && ok "ready-job lookup can use the partial index" || { echo "$plan"; exit 1; }

echo
echo "sql tests: $pass passed"
