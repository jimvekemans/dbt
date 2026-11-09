#!/usr/bin/env bash
# End-to-end check of full_refresh_strategy: swap and chunked rebuilds on SingleStore.
#
# Usage (from the repo root, with the SingleStore ADBC driver wired up, see SKILL.md):
#   SINGLESTORE_PASSWORD=... .agents/skills/singlestore-chunked-reloads/scripts/smoke.sh [--dbt PATH] [--keep]
#
# Env: SINGLESTORE_HOST (127.0.0.1), SINGLESTORE_PORT (3306), SINGLESTORE_USER (root),
#      SINGLESTORE_PASSWORD (required), SINGLESTORE_DATABASE (dbt_chunk_smoke),
#      SINGLESTORE_CONTAINER (singlestore): used with `docker exec` to create and drop
#      the database. Without that container, create the database yourself first.
#
# Builds a throwaway project into SINGLESTORE_DATABASE and prints PASS/FAIL per check.
# Exit 0 when every check passes. --keep keeps the project dir and the database.
set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
dbt="$repo_root/target/debug/dbt"
keep=0
while [ $# -gt 0 ]; do
  case "$1" in
    --dbt) dbt="$(realpath "$2")"; shift 2 ;;
    --keep) keep=1; shift ;;
    -h|--help) sed -n '2,14p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
: "${SINGLESTORE_HOST:=127.0.0.1}" "${SINGLESTORE_PORT:=3306}" "${SINGLESTORE_USER:=root}"
: "${SINGLESTORE_DATABASE:=dbt_chunk_smoke}" "${SINGLESTORE_CONTAINER:=singlestore}"
if [ -z "${SINGLESTORE_PASSWORD:-}" ]; then echo "set SINGLESTORE_PASSWORD" >&2; exit 2; fi
export SINGLESTORE_PASSWORD
if [ ! -x "$dbt" ]; then echo "no dbt at $dbt; run: cargo build -p dbt-sa-cli -j 6" >&2; exit 2; fi

sql() { # run SQL in the container; quiet unless it fails
  docker exec -i "$SINGLESTORE_CONTAINER" singlestore -u"$SINGLESTORE_USER" -p"$SINGLESTORE_PASSWORD" \
    -N -e "$1" 2> >(grep -v -e 'Using a password' -e getopt >&2)
}
if docker inspect "$SINGLESTORE_CONTAINER" > /dev/null 2>&1; then
  sql "create database if not exists $SINGLESTORE_DATABASE" || exit 2
  have_container=1
else
  have_container=0
fi

dir="$(mktemp -d -t dbt-ss-chunks.XXXXXX)"
log="$dir/smoke.log"
mkdir -p "$dir/models" "$dir/seeds"
failures=0
check() { if [ "$2" -eq 0 ]; then echo "PASS $1"; else echo "FAIL $1 (see $log)"; failures=$((failures + 1)); fi; }
run() { echo "\$ dbt $*" >> "$log"; "$dbt" "$@" --project-dir "$dir" --profiles-dir "$dir" >> "$log" 2>&1; }
rows() { # rows <sql>: the JSON rows of a query (the last stdout line: SingleStore prints a debug warning first)
  "$dbt" show --inline "$1" --output json -q --project-dir "$dir" --profiles-dir "$dir" 2>> "$log" | tail -n 1
}

cat > "$dir/dbt_project.yml" <<'EOF'
name: ss_chunks
version: '1.0'
config-version: 2
profile: ss_chunks
EOF
cat > "$dir/profiles.yml" <<EOF
ss_chunks:
  target: dev
  outputs:
    dev:
      type: singlestore
      host: $SINGLESTORE_HOST
      port: $SINGLESTORE_PORT
      user: $SINGLESTORE_USER
      password: "{{ env_var('SINGLESTORE_PASSWORD') }}"
      database: $SINGLESTORE_DATABASE
      schema: $SINGLESTORE_DATABASE
      threads: 1
EOF
# 20 customers (every 5th inactive), 200 orders: 160 orders of 16 active customers, sum 8261.
python3 -I - "$dir/seeds" <<'EOF'
import random, sys
random.seed(7)
with open(f"{sys.argv[1]}/raw_customers.csv", "w") as f:
    f.write("customer_id,status\n")
    for c in range(1, 21):
        f.write(f"{c},{'active' if c % 5 else 'inactive'}\n")
with open(f"{sys.argv[1]}/raw_orders.csv", "w") as f:
    f.write("order_id,customer_id,amount\n")
    for o in range(1, 201):
        f.write(f"{o},{random.randint(1, 20)},{random.randint(1, 100)}\n")
EOF
# Chunk filters on the join key in both inputs.
cat > "$dir/models/fct_orders.sql" <<'EOF'
with orders as (
    select * from {{ ref('raw_orders') }}
    {{ chunk_where('customer_id') }}
),
customers as (
    select * from {{ ref('raw_customers') }}
    where status = 'active'
    {{ chunk_and('customer_id') }}
)
select o.order_id, o.customer_id, o.amount
from orders o join customers c on o.customer_id = c.customer_id
EOF
# No chunk filters: the output is split on chunk_output_by.
cat > "$dir/models/agg_customer.sql" <<'EOF'
select customer_id, count(*) as n_orders, sum(amount) as total
from {{ ref('fct_orders') }}
group by customer_id
EOF
schema_yml='version: 2
models:
  - name: fct_orders
    config:
      materialized: incremental
      unique_key: order_id
      full_refresh_strategy: swap
      chunks: 4
    columns:
      - name: order_id
        data_tests: [unique, not_null]
  - name: agg_customer
    config:
      materialized: table
      full_refresh_strategy: swap
      chunks: 3
      chunk_output_by: customer_id
    columns:
      - name: customer_id
        data_tests: [unique]'
printf '%s\n' "$schema_yml" > "$dir/models/schema.yml"

echo "dbt:      $dbt"
echo "project:  $dir"
echo "database: $SINGLESTORE_DATABASE on $SINGLESTORE_HOST:$SINGLESTORE_PORT"

run debug && grep -q 'All checks passed' "$log"
check "debug: driver loads and connects" $?

: > "$log.build"; echo "\$ dbt build" >> "$log"
"$dbt" build --project-dir "$dir" --profiles-dir "$dir" > "$log.build" 2>&1; rc=$?; cat "$log.build" >> "$log"
check "build exits 0" $rc
[ "$(grep -c 'Loading chunk [0-9]/4 of `'"$SINGLESTORE_DATABASE"'`.`fct_orders` (into `[^`]*`.`fct_orders__dbt_tmp_' "$log.build")" -eq 4 ]
check "fct_orders loads 4 chunks into a shadow table" $?
grep -q 'Tested `'"$SINGLESTORE_DATABASE"'`.`fct_orders` (in `[^`]*`.`fct_orders__dbt_tmp_[^`]*`) before the swap: 2 tests passed' "$log.build"
check "build tests fct_orders on the shadow before the swap" $?
[ "$(grep -c 'Loading chunk [0-9]/3 of `'"$SINGLESTORE_DATABASE"'`.`agg_customer`' "$log.build")" -eq 3 ]
check "agg_customer loads 3 chunks (chunk_output_by)" $?

[ "$(rows "select count(*) n, sum(amount) s from {{ ref('fct_orders') }}")" = '[{"n":160,"s":8261}]' ]
check "fct_orders has all 160 rows (sum 8261)" $?
[ "$(rows "select count(*) n, sum(total) s from {{ ref('agg_customer') }}")" = '[{"n":16,"s":8261}]' ]
check "agg_customer has 16 customers (sum 8261)" $?

: > "$log.inc"; "$dbt" run -s fct_orders --project-dir "$dir" --profiles-dir "$dir" > "$log.inc" 2>&1; rc=$?; cat "$log.inc" >> "$log"
[ "$rc" -eq 0 ] && ! grep -q 'Loading chunk' "$log.inc"
check "plain incremental run: no chunks, no swap" $?

: > "$log.ver"; DBT_ENGINE_FEATURES=verify_chunked_rebuilds "$dbt" build -s fct_orders --full-refresh \
  --project-dir "$dir" --profiles-dir "$dir" > "$log.ver" 2>&1; rc=$?; cat "$log.ver" >> "$log"
[ "$rc" -eq 0 ] && grep -q 'Verified `[^`]*`.`fct_orders` .*160 rows, the same as without chunk filters' "$log.ver"
check "verify_chunked_rebuilds accepts correct chunk filters" $?

# A filter on the wrong column silently drops rows; only the preview flag catches it.
sed -i "s/chunk_where('customer_id')/chunk_where('order_id')/" "$dir/models/fct_orders.sql"
: > "$log.bad"; DBT_ENGINE_FEATURES=verify_chunked_rebuilds "$dbt" build -s fct_orders --full-refresh \
  --project-dir "$dir" --profiles-dir "$dir" > "$log.bad" 2>&1; rc=$?; cat "$log.bad" >> "$log"
[ "$rc" -eq 1 ] && grep -q 'ChunkVerificationFailed (dbt1423)' "$log.bad"
check "verify_chunked_rebuilds fails a wrong chunk column (dbt1423)" $?
[ "$(rows "select count(*) n from {{ ref('fct_orders') }}")" = '[{"n":160}]' ]
check "...and the live table keeps its 160 rows" $?
sed -i "s/chunk_where('order_id')/chunk_where('customer_id')/" "$dir/models/fct_orders.sql"

# A data test failing on the shadow keeps the live relation.
cat >> "$dir/models/schema.yml" <<'EOF'
      - name: n_orders
        data_tests:
          - accepted_values:
              arguments:
                values: [1]
EOF
: > "$log.test"; "$dbt" build -s agg_customer --project-dir "$dir" --profiles-dir "$dir" > "$log.test" 2>&1; rc=$?
cat "$log.test" >> "$log"
[ "$rc" -eq 1 ] && grep -q 'SwapPreSwapTestFailed (dbt1422)' "$log.test"
check "failing test on the shadow stops the swap (dbt1422)" $?
[ "$(rows "select count(*) n, sum(total) s from {{ ref('agg_customer') }}")" = '[{"n":16,"s":8261}]' ]
check "...and agg_customer is unchanged" $?
printf '%s\n' "$schema_yml" > "$dir/models/schema.yml"

# chunks without swap: loads straight into the live relation.
sed -i '/name: agg_customer/,/chunk_output_by/ s/full_refresh_strategy: swap/full_refresh_strategy: in_place/' "$dir/models/schema.yml"
: > "$log.inp"; "$dbt" build -s agg_customer --project-dir "$dir" --profiles-dir "$dir" > "$log.inp" 2>&1; rc=$?
cat "$log.inp" >> "$log"
[ "$rc" -eq 0 ] && [ "$(grep -c 'Loading chunk [0-9]/3 of `[^`]*`.`agg_customer`$' "$log.inp")" -eq 3 ] \
  && [ "$(rows "select count(*) n, sum(total) s from {{ ref('agg_customer') }}")" = '[{"n":16,"s":8261}]' ]
check "in_place chunks load into the live table (16 rows)" $?
printf '%s\n' "$schema_yml" > "$dir/models/schema.yml"

if [ "$have_container" -eq 1 ]; then
  leftovers="$(sql "select count(*) from information_schema.tables where table_schema = '$SINGLESTORE_DATABASE' and (table_name like '%\\_\\_dbt\\_tmp%' or table_name like '%\\_\\_dbt\\_backup%')")"
  echo "leftover shadow/backup tables: ${leftovers:-query failed}" >> "$log"
  [ "$leftovers" = "0" ]
  check "no __dbt_tmp / __dbt_backup tables left behind" $?
fi

echo
if [ "$failures" -eq 0 ]; then echo "ALL CHECKS PASSED"; else echo "$failures CHECK(S) FAILED"; fi
rm -f "$log".build "$log".inc "$log".ver "$log".bad "$log".test "$log".inp
if [ "$keep" -eq 1 ] || [ "$failures" -ne 0 ]; then
  echo "kept project $dir and database $SINGLESTORE_DATABASE"
else
  rm -rf "$dir"
  [ "$have_container" -eq 1 ] && sql "drop database if exists $SINGLESTORE_DATABASE"
fi
[ "$failures" -eq 0 ]
