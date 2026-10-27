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

`chunks` and `chunk_by` are rejected at parse time unless `full_refresh_strategy: swap`
is set.

## When it runs

| Model | A swap rebuild happens when |
|---|---|
| `table` | every run |
| `incremental` | `--full-refresh` (or `full_refresh: true`), or the relation does not exist yet |
| `incremental` with `incremental_strategy: microbatch` | same as incremental; the batches are the chunks |

An ordinary incremental run is unchanged: it updates the live relation in place.

## What happens

1. The shadow and backup are named by `make_intermediate_relation` and
   `make_backup_relation`: the same `__dbt_tmp` and `__dbt_backup` relations the table
   materialization uses. Whatever an interrupted run left under those names is dropped
   first.
2. Pre-hooks run once, with `{{ this }}` = the live relation.
3. The first chunk runs through the model's own materialization, pointed at the shadow,
   so contracts, constraints, grants, `persist_docs` and adapter DDL configs (SingleStore
   `shard_key`, Snowflake `cluster_by`, ...) all apply.
4. The other chunks are inserted into the shadow, with the column list read from it.
   Microbatch models run their batches against the shadow instead. Nobody reads the
   shadow, so each chunk commits on its own.
5. The shadow is swapped in and the old data dropped. On SingleStore, which keys
   privileges by table name, grants are applied to the live relation here instead of
   to the shadow in step 3.
6. Post-hooks run once, with `{{ this }}` = the live relation.

If anything fails before the swap, the shadow is dropped and the live relation is left as
it was. `{{ this }}` in the model SQL always means the live relation.

For a `table` model with `chunks: 1`, the data is still written once; the swap adds two
renames and moves the hooks around the swap.

## Chunks

`chunks: N` splits the model output into `N` hash buckets on `chunk_by`, which defaults to
`unique_key` when that is a single column (a string, or a list of one). Like `unique_key`, `chunk_by` goes into the SQL
as written: quote it yourself if it is mixed-case or a reserved word.

- Rows with a NULL key load with chunk 0. Every row lands in exactly one chunk.
- Every chunk runs the whole model SQL and keeps its own bucket. That trades scan time for
  peak memory, and it needs deterministic SQL: `current_timestamp`, `random()` or sources
  that change mid-run can make chunks disagree.
- Microbatch models are already split by `batch_size` and do not accept `chunks`.

Each adapter ships a default hash expression, except Salesforce, Exasol, Datafusion,
Dremio, Oracle and LakeCompute: override `get_chunk_predicate` to use `chunks` there.

## The swap

| Adapter | Swap | Readers see |
|---|---|---|
| Snowflake | `alter table ... swap with ...` | the old or the new table |
| Postgres, Redshift, DuckDB | both renames in one transaction | the old or the new table |
| all others, including SingleStore | rename live → backup, then shadow → live | briefly, no table: queries in that moment fail |

A live view (say the model used to be a view) is dropped, and the shadow renamed in.

## Overriding the SQL

dbt runs the rebuild. The two pieces of SQL that differ per warehouse or project are
dispatched macros in the `dbt` namespace:

| Macro | Returns |
|---|---|
| `get_chunk_predicate(column, chunks, chunk)` | a SQL boolean selecting chunk `chunk` (0-based) |
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
