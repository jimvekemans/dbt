# Full refresh by swap

`full_refresh_strategy: swap` rebuilds a `table` or `incremental` model next to the
live relation, and swaps it in only after the whole rebuild succeeded. With `chunks`,
the rebuild loads the model in several statements instead of one.

Use it when readers must not see a missing or half-built relation during a rebuild, or
when one `create table as select` over the whole model runs out of warehouse memory.
How close to atomic the final swap is depends on the adapter; see
[The swap](#the-swap).

```yaml
models:
  - name: fct_orders
    config:
      materialized: incremental
      unique_key: order_id
      full_refresh_strategy: swap   # default: in_place
      chunks: 16                    # default: 1
```

`chunks` also works without `swap` (`full_refresh_strategy: in_place`, the default): see
[Chunked rebuilds in place](#chunked-rebuilds-in-place). Set these configs in YAML: a typo in
an SQL `config()` call fails with an error that does not point at the typo.

## When it runs

| Model | A swap rebuild happens when |
|---|---|
| `table` | every run |
| `incremental` | `--full-refresh` (or `full_refresh: true`), or the relation does not exist yet |
| `incremental` with `incremental_strategy: microbatch` | same as incremental; the batches are the chunks |

An ordinary incremental run is unchanged: it updates the live relation in place.

> [!NOTE]
> Running `--full-refresh` on a microbatch model with a narrow event-time window (e.g. `--event-time-start` and `--event-time-end`) replaces the entire live table with only that window's batches, identical to non-swap microbatch behavior. Swap does not merge back historical data outside the specified window.

## What happens

1. The shadow and backup are named by `make_intermediate_relation` and
   `make_backup_relation`: they carry unique per-invocation suffixes
   (`<model>__dbt_tmp_<suffix>` and `<model>__dbt_backup_<suffix>`), preventing collisions
   across concurrent runs. Whatever this run's invocation names target is dropped first
   if present, while lingering leftover tables from earlier failed runs older than 24 hours
   are cleaned up at run start.
2. Pre-hooks run once, with `{{ this }}` = the live relation.
3. The first chunk runs through the model's own materialization, pointed at the shadow,
   so contracts, constraints, grants, `persist_docs` and adapter DDL configs (SingleStore
   `shard_key`, Snowflake `cluster_by`, ...) all apply. With
   [`chunk_verification: sampled`](#verifying-chunk-filters), it is verified right away.
4. The other chunks are inserted into the shadow, with the column list read from it.
   Microbatch models run their batches against the shadow instead. Nobody reads the
   shadow, so each chunk commits on its own. Every chunk logs `Loading chunk k/N of
   <relation> (into <shadow>)` (`Loading chunk k/N of <relation>` for in-place builds).
5. With [`chunk_verification: full`](#verifying-chunk-filters), the whole shadow is
   verified.
6. When the invocation selected the model's data tests (`dbt build`), they run against the
   shadow; see [Testing before the swap](#testing-before-the-swap).
7. The shadow is swapped in and the old data dropped. On SingleStore, which keys
   privileges by table name, grants are applied to the live relation here instead of
   to the shadow in step 3.
8. Post-hooks run once, with `{{ this }}` = the live relation.

If anything fails before the swap, including a test in step 6, the shadow is dropped and the
live relation is left as it was. `{{ this }}` in the model SQL always means the live relation.

For a `table` model, step 3 delegates to the adapter's `table` materialization pointed at the
shadow, which builds into its own intermediate relation (`<model>__dbt_tmp_<suffix>__dbt_tmp_<suffix>`)
and renames it to `<model>__dbt_tmp_<suffix>` before step 7 swaps `<model>__dbt_tmp_<suffix>` in.
The doubled suffix adds up to 42 characters on SingleStore (whose identifier limit is 256
characters) or 18 characters on adapters with a plain `__dbt_tmp` suffix, and the 24-hour
cleanup matches these intermediate names as well.

## Chunks

`chunks: N` loads the model in `N` hash buckets. The buckets come from
[chunk filters in the model SQL](#chunk-filters-in-the-model-sql) when it has them, which is
what you want. Without them, dbt splits the model's output on `chunk_output_by`, which defaults
to `unique_key` when that is a single column (a string, or a list of one), and warns
(`dbt1420`) unless `chunk_output_by` is set explicitly. Like `unique_key`, `chunk_output_by`
goes into the SQL as written: quote it yourself if it is mixed-case or a reserved word.

- Rows with a NULL key load with chunk 0. Every row lands in exactly one chunk.
- Every chunk runs the whole model SQL and keeps its own bucket. That trades scan time for
  peak memory, and it needs deterministic SQL: `current_timestamp`, `random()` or sources
  that change mid-run can make chunks disagree. To filter the inputs instead of the output,
  use [chunk filters](#chunk-filters-in-the-model-sql).
- Microbatch models are already split by `batch_size` and do not accept `chunks`.

### Chunk filters in the model SQL

Filtering the output still makes every chunk read and join all of its inputs. Some engines
(SingleStore among them) handle a complex query much better when the inputs are filtered
before the joins. `chunk_where(column)` and `chunk_and(column)` put a chunk predicate in the
model SQL itself, typically in each import CTE:

```sql
with orders as (
    select * from {{ ref('stg_orders') }}
    {{ chunk_where('customer_id') }}               -- "where <predicate>", or nothing
),
customers as (
    select * from {{ ref('stg_customers') }}
    where status = 'active'
    {{ chunk_and('customer_id') }}                 -- "and <predicate>", or nothing
),
stores as (
    select * from {{ ref('stg_stores') }}          -- a lookup: no filter, read in full by every chunk
)
...
```

Both are built on `get_chunk_filter(column)`, which returns the bare predicate or none.
During a chunked rebuild they compile to the predicate for chunk 0, tagged with its column:

```sql
where (customer_id is null or crc32(customer_id) % 8 = 0) /* dbt_chunk: customer_id */
```

dbt rewrites each tagged predicate to the one for chunk `k` (`get_chunk_predicate(column,
chunks, k)`) and runs the SQL without an output filter, so `chunk_output_by` is not needed.
Outside a chunked rebuild (`chunks` unset or 1, or an incremental run) they render nothing.

- Filter every input that holds the model's grain key, each on the column that carries that
  key *in that input*: the column in the join condition, which may be named differently in
  each input (`hub_vacature_hkey` in one, `vacature_uuid_hkey` in the other). Rows that are
  joined together then share a chunk. Inputs without a filter are read in full by every
  chunk. A wrong column does not fail the run: joins silently miss rows. Turn on
  [`chunk_verification`](#verifying-chunk-filters) to catch it.
- A model that aggregates or uses window functions must filter on its group or partition
  key, or a part of it.
- The column may be an expression, such as `concat_ws('|', a, b)` for a composite key; prefer
  a single hash key.
- Every chunk scans its filtered inputs in full: hash predicates do not prune segments or
  partitions. What drops to `1/N` is the memory of the joins and aggregations.
- Render the predicates with these macros only: dbt fails the rebuild when a tag's
  predicate does not match `get_chunk_predicate(column, chunks, 0)`.
- `dbt compile` and `dbt show` of a model in a chunked rebuild show chunk 0 only.

### Chunked rebuilds in place

Without `swap`, `chunks` loads the rebuild straight into the live relation: the first chunk
replaces it through the model's own materialization, the other chunks are inserted, and the
post-hooks run after the last one. It needs no room for a second copy, but readers see a
partial relation until the last chunk is in, and tests can only run after it.

When a chunk after the first fails, dbt drops the partial relation (`dbt1419`), so the next run
rebuilds it from scratch instead of updating a partial table incrementally. Prefer `swap`
wherever there is room for a second copy.

### Testing before the swap

When an invocation selects the data tests of a `swap` model, as `dbt build` does, dbt runs
them against the shadow before the swap: each test is rendered as usual and pointed at the
shadow by replacing the model's relation name in its SQL. A failing test (severity `error`,
or a warning promoted by `--warn-error`) fails the model with `dbt1422` and keeps the live
relation. The tests then run again as usual after a successful swap. `dbt run` selects no
tests, so it swaps without testing. When a selected test also reads another node in the run
(such as a `relationships` or `equals_relation` target), dbt schedules that node before the
`swap` model whenever doing so does not introduce a cycle. A test that does not mention the
model's relation by name cannot be redirected, and a test that also reads a downstream node in
the run that has not finished yet would compare against stale data; both are skipped before the
swap (`dbt1421`) and run after it.

### Verifying chunk filters

A wrong chunk filter produces wrong data without failing. `chunk_verification` makes dbt
check the filters while it rebuilds: with correct filters the result does not depend on how
the model is chunked, so dbt builds some of its rows again under a different bucketing and
compares the two. It compares fingerprints (the row count and the sum of a hash of every
row), not rows, and never runs the model without its chunk filters, so no verification
statement needs more memory than a chunk of the load. A difference fails the model with
`dbt1423`, before the swap: the live relation is kept (in place, the partial relation is
dropped, as for any failed chunk).

| `chunk_verification` | When | What is compared | Extra work |
|---|---|---|---|
| `sampled` | right after the first chunk loads | that chunk vs. the same rows built as chunks 1 and N+1 of 2N | two half chunks |
| `full` | after the last chunk | the whole result vs. the model built in N+1 chunks | N+1 chunks, each smaller than a chunk of the load |
| `off` | never | | none |

Set it per model, per folder (`+chunk_verification`) or per target:

```yaml
# dbt_project.yml
models:
  my_project:
    marts:
      +chunk_verification: "{{ 'sampled' if target.name == 'ci' else 'off' }}"

# a model's YAML
models:
  - name: fct_orders
    config:
      full_refresh_strategy: swap
      chunks: 8
      chunk_verification: full
```

When a model does not set it, the preview flag `verify_chunked_rebuilds` makes it `sampled`
(`DBT_ENGINE_FEATURES=verify_chunked_rebuilds dbt build ...`); without the flag it is `off`.
A bare `off` in YAML reads as `false`, which also means `off` (`true` means `sampled`). On a
model without chunks or without chunk filters it has no effect (splitting the output is
correct by construction), so it is safe to set on a whole folder.

What it catches. A misaligned filter shows up as rows that belong together landing in
different chunks:

- `sampled` catches a filter on the wrong column almost surely, and fails after the first
  chunk instead of after the whole load. It always checks the first chunk, so repeated runs
  cover the same keys: isolated mismatches between keys need `full`.
- `full` checks every row, and catches each misaligned pair of rows with probability
  (2N-1)/(N(N+1)), about 21% at N=8, so any systematic error.
- Both need deterministic output (no `current_timestamp()`, `random()`, or ties in a
  window's `order by`; take timestamps from `run_started_at`) and inputs that do not change
  during the rebuild.
- `sampled` needs a custom `get_chunk_predicate` to nest: chunk k of N must be chunks k
  and k+N of 2N, as with any `expression % chunks`. Use `full` otherwise.
- `full` builds chunks of N+1. With heavily skewed keys one of them can be larger than every
  chunk of the load; a verification statement that fails says so.
- Values are hashed as the model SQL returns them and as the relation stores them. The first
  chunk creates the relation from the same SQL, so they agree, unless a contract declares a
  column with less precision than the SQL produces (`datetime` for a `datetime(6)` value):
  declare the type the SQL returns, or cast it in the SQL.

The row hash comes from the dispatched `get_rows_fingerprint(sql, columns)` macro (see
[Overriding the SQL](#overriding-the-sql)).

Each adapter ships a default hash expression, except Salesforce, Exasol, Datafusion,
Dremio, Oracle and LakeCompute: override `get_chunk_predicate` to use `chunks` there.

## The swap

By default, `swap_relations` drops any lingering backup table, renames the live table to
the backup name, and renames the shadow table to the live name (restoring the backup back
to the live name if the second rename fails), then drops the backup table last. Readers
can briefly find no table between the two renames. Projects on warehouses with a
single-statement or transactional swap can override the `swap_relations` macro (see
[Overriding the SQL](#overriding-the-sql)).

A live view (say the model used to be a view) is dropped, and the shadow renamed in.

## Overriding the SQL

dbt runs the rebuild. The pieces of SQL that differ per warehouse or project are
dispatched macros in the `dbt` namespace:

| Macro | Returns |
|---|---|
| `get_chunk_predicate(column, chunks, chunk)` | a SQL boolean selecting chunk `chunk` (0-based) |
| `get_rows_fingerprint(sql, columns)` | a query returning `(row_count, row_hash_sum)` for the rows of `sql` |
| `swap_relations(relation, shadow, backup)` | nothing; performs the swap |

Override them like any dispatched macro: a macro in your project named
`<adapter>__<name>` overrides it for that adapter, and one named `default__<name>` for
every adapter. The built-in default stays callable as `default__<name>`, as below. Packages can ship overrides through the
usual [`dispatch` config](https://docs.getdbt.com/reference/project-configs/dispatch-config).

```sql
-- macros/get_chunk_predicate.sql
-- fct_events has an integer surrogate key: plain modulo spreads it evenly.
{% macro singlestore__get_chunk_predicate(column, chunks, chunk) %}
  {% if model.name != 'fct_events' %}
    {{ return(default__get_chunk_predicate(column, chunks, chunk)) }}
  {% endif %}
  {% if chunk == 0 %}
    {{ return("(" ~ column ~ " is null or " ~ column ~ " % " ~ chunks ~ " = 0)") }}
  {% endif %}
  {{ return("(" ~ column ~ " is not null and " ~ column ~ " % " ~ chunks ~ " = " ~ chunk ~ ")") }}
{% endmacro %}
```

A `swap_relations` override must handle a first run, where `relation` does not exist
(`adapter.get_relation(...)` returns `none`). To change the shadow and backup names,
override `make_intermediate_relation` or `make_backup_relation`; there is no separate
naming hook.

## Limitations

- On adapters without a one-statement or transactional swap (including SingleStore),
  queries that hit the moment between the two renames fail.
- Only one process at a time may rebuild a model: concurrent rebuilds share the shadow.
- A rebuild needs room for two copies of the model.
- Python models are not supported.
- Splitting one rebuild across several dbt invocations (one chunk per orchestrator task) is
  out of scope: it needs state that outlives the invocation.

## Failure handling and operator recovery

- **Failure before swap**: If chunk execution or batch loading fails before the swap, `abort` drops the shadow relation (`<model>__dbt_tmp_<suffix>`). The live target relation was never touched.
- **Failure during swap (rollback succeeded)**: On two-step rename adapters (e.g., SingleStore), the live target is renamed to `<model>__dbt_backup_<suffix>`, then `<model>__dbt_tmp_<suffix>` is renamed to `<model>`. If the second rename fails, dbt attempts to restore `<model>__dbt_backup_<suffix>` back to `<model>`. If this rollback succeeds, the live relation retains its original data and the shadow table is dropped.
- **Failure during swap (rollback failed / view upgrade)**: If the rollback rename fails, or if a pre-existing view was dropped before the rename failed, dbt preserves both `<model>__dbt_tmp_<suffix>` and `<model>__dbt_backup_<suffix>` to prevent unrecoverable data loss.
  - The error message from `swap_relations` prints the exact table names:
    ```
    Failed to swap shadow table '<model>__dbt_tmp_<suffix>' to target '<model>' (...). Attempted rollback to restore backup table '<model>__dbt_backup_<suffix>' to target '<model>' also failed (...). Old data remains in backup table '<model>__dbt_backup_<suffix>' and new data in shadow table '<model>__dbt_tmp_<suffix>'. Manual operator intervention required.
    ```
  - `<model>__dbt_backup_<suffix>` holds the original pre-swap table data.
  - `<model>__dbt_tmp_<suffix>` holds the newly built post-swap table data.
  - **Operator Recovery**:
    - Identify the suffixed table names from the error message or by querying `information_schema.tables WHERE table_name LIKE '<model>__dbt_backup%'`.
    - To restore old data: rename `<model>__dbt_backup_<suffix>` to `<model>` (and drop `<model>__dbt_tmp_<suffix>` once verified).
    - To promote new data: rename `<model>__dbt_tmp_<suffix>` to `<model>` (and drop `<model>__dbt_backup_<suffix>` once verified).
  - Note: Lingering `<model>__dbt_backup` tables are only cleared at the start of a build (`begin`) when the live target table exists. If a previous run failed mid-swap leaving only the backup (and no target), `<model>__dbt_backup_<suffix>` is preserved so that any subsequent build failure will not cause data loss. Operators should complete manual recovery before re-running the model.
