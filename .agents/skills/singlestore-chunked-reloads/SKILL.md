---
name: singlestore-chunked-reloads
description: How `full_refresh_strategy: swap` and chunked rebuilds (`chunks`, `chunk_where`/`chunk_and`/`get_chunk_filter`, `chunk_output_by`, `verify_chunked_rebuilds`) work inside this repo, and how to run, change and verify them on a local SingleStore. Use this whenever you read, debug, review or change the swap/chunk code (`full_refresh_swap.rs`, `chunking.rs`, `swap_relations`, `test_shadow_before_swap`, the SingleStore materializations) or run dbt against SingleStore. Also use it for zero-downtime full refreshes, backfills, batched or chunked reloads, `__dbt_tmp_`/`__dbt_backup_` leftovers, or errors dbt1415 to dbt1424, even when the user only says "reload in batches" or "the full refresh runs out of memory".
---

Branch `feat/backfill-hardening-pr` adds two model configs on top of the native
SingleStore adapter. `full_refresh_strategy: swap` rebuilds a `table`/`incremental`
model into a shadow table and swaps it in only after the rebuild succeeded.
`chunks: N` loads a rebuild in `N` hash buckets. The user-facing contract is
`docs/full-refresh-swap.md`; keep it in sync when behaviour changes. This skill is
the developer's view: how the code is wired, why it is shaped that way, and how
to prove a change works. Paths are relative to the repo root.

## How a rebuild flows

1. **Parse**: `dbt-parser/src/resolve/resolve_models.rs` calls
   `validate_rebuild_config` (`dbt-schemas/src/schemas/full_refresh_swap.rs`), so bad
   configs fail at parse time. The same file turns config into a `SwapPlan
   { chunks, chunk_output_by }` via `swap_plan` (for `swap`) or `in_place_chunk_plan`
   (`chunks` without swap). `chunk_output_by` falls back to a single-column
   `unique_key`. Config fields live in `project/configs/model_config.rs`.
2. **Compile**: the model SQL renders once. `get_chunk_filter(column)`
   (`dbt-loader/.../dbt-adapters/macros/adapters/full_refresh_swap.sql`) emits the
   **chunk-0** predicate plus the tag `/* dbt_chunk: <column> */`, but only when
   `chunks > 1`, the model is table/incremental, and `not is_incremental()`.
   `chunk_where`/`chunk_and` wrap it.
3. **Dispatch**: `dbt-tasks-sa/src/runnable/model.rs` picks the path.
   `rebuilds_from_scratch` (table: always; incremental: `--full-refresh` or no target
   yet) gates both plans. Then it calls `materialize_model_swap`,
   `materialize_model_chunked_in_place`, or the untouched `materialize_model`.
   Microbatch models use `begin_microbatch_swap` / `finish_microbatch_swap`
   (wired in `runnable/runnable/mod.rs`): the batches run against
   `ShadowBuild::shadow_model`.
4. **Swap path** (`dbt-tasks-sa/src/full_refresh_swap.rs`, `ShadowBuild`):
   - `begin`: names the target, shadow and backup through the dispatched
     `make_intermediate_relation` and `make_backup_relation` macros, under the
     context keys `__dbt_swap_target`, `__dbt_swap_shadow` and `__dbt_swap_backup`.
     It drops a stale shadow (and the backup, if the target exists), then runs the
     pre-hooks against the real target.
   - `load`: runs chunk 0 through the model's own materialization, using
     `shadow_model()`, a clone with `alias` = the shadow, no hooks, and no grants
     where they don't survive a rename. Then `insert_remaining_chunks` inserts
     chunks 1..N with an explicit column list, and each chunk commits.
   - `verify`, under the flag: see step 6.
   - `before_swap`: `test_shadow_before_swap` (step 7).
   - `finish`: the `swap_relations` macro, then `apply_grants` on the target if
     `!grants_follow_rename`, then the post-hooks.
   - `abort`, on any error: drops the shadow, or keeps it when the swap left the
     target missing (`dbt1425`).
5. **Chunk SQL** (`chunk_sql`): when the SQL carries tags, `rewrite_chunk_filters`
   string-replaces each exact chunk-0 predicate with `get_chunk_predicate(column,
   chunks, k)`. Otherwise it wraps the SQL in `select * from (...) where
   <predicate on chunk_output_by>`. The predicates come from the
   `get_chunk_predicate` macro, whose default is `Adapter::chunk_predicate`, which
   calls `dbt-adapter-sql/src/chunking.rs` (`crc32(col) % N` on SingleStore). NULL
   keys always go to chunk 0.
6. **Verify** (`verify_chunks`): only when `dbt_flags::VERIFY_CHUNKED_REBUILDS` is on
   (env `DBT_ENGINE_FEATURES=verify_chunked_rebuilds`), and only for models with
   tags. It counts rows in the loaded relation and in the SQL with every tag
   rewritten to `(1 = 1)`, and runs `except` in both directions. Any difference
   raises `dbt1423`.
7. **Pre-swap tests** (`runnable/test.rs::test_shadow_before_swap`): it takes the
   selected tests that depend on the model, renders each as usual, and
   `replace_relation`s the live name with the shadow name in the SQL text.
   - A test that never names the relation, or reads an unfinished node, is
     skipped (`dbt1421`).
   - A failure raises `dbt1422`, and the live table is kept.
   - `graph.rs::add_swap_pre_test_dependencies` schedules those other nodes before
     the swap model, when that adds no cycle.
8. **The swap** (`AdapterImpl::swap_relations`, `dbt-adapter/src/adapter/adapter_impl.rs`):
   - If there is no target, rename shadow to target.
   - If the target is a view, drop it, then rename.
   - If it's a table: drop the backup, rename target to backup, rename shadow to
     target. If that second rename fails, rename the backup back to the target.
     If the restore also fails, the error names both leftover tables.
   - Every step updates the relation cache. `Adapter::rename_relation` runs the
     warehouse rename *before* `cache_renamed`, so a failed rename leaves the
     cache alone.

## Why it is built this way

- **Rewriting text, not re-rendering.** The model compiles once. Chunks differ only
  in their predicates, so `rewrite_chunk_filters` replaces strings. It requires
  every tag to carry the exact chunk-0 predicate, and rejects anything
  hand-written (`InvalidConfig`). A change to `get_chunk_filter`'s output format
  has to change `CHUNK_FILTER_TAG_*` and `chunk_filter` with it.
- **Chunk 0 goes through the real materialization**, so contracts, constraints,
  `persist_docs` and adapter DDL configs (SingleStore `shard_key`, ...) apply to the
  shadow. Chunks 1..N are plain inserts, which therefore prepend `sql_header`
  themselves.
- **SingleStore grants**: privileges are keyed by table name, so
  `grants_follow_rename(SingleStore) == false` (`dbt-adapter-core/src/lib.rs`).
  The shadow is built without grants, and they're applied to the target after the swap.
- **Names are per invocation.** SingleStore's
  `singlestore__make_intermediate_relation`/`__make_backup_relation`
  (`dbt-singlestore/macros/relations.sql`) append an invocation-based suffix.
  `singlestore__drop_stale_temp_and_backup_relations` (called by the SingleStore
  `table.sql`/`incremental.sql`) drops leftovers older than 24 h, so a retry never
  clobbers the backup a failed run left behind. A `table` swap model builds through
  `table.sql`, which makes its own intermediate inside the shadow name
  (`<m>__dbt_tmp_<s>__dbt_tmp_<s>`) and renames it to the shadow.
- **Override points are dispatched macros** (`get_chunk_predicate`,
  `swap_relations`, `make_intermediate_relation`, `make_backup_relation`).
  Projects override them like any dispatched macro. The Rust side never hardcodes
  that SQL.

## Changing it

- New adapter hash: add an arm to `chunk_bucket_expr` in `chunking.rs`.
  `AdapterType` matches are exhaustive, so `None` means "project must override".
- New or changed warning/error: add a code in `dbt-error/src/codes.rs` (the range is
  1415–1424 today, one code per condition). Never `Generic`.
- Flags: `verify_chunked_rebuilds` is declared in `dbt-flags/src/catalog.rs`. Read
  it with `dbt_flags::VERIFY_CHUNKED_REBUILDS.enabled()`; no `std::env::var`.
- Logging: use the `emit_*_log_message` helpers, as the existing code does. The
  `Loading chunk k/N of <target> (into <shadow>)` and `Verified ...` lines are
  user-facing (and the smoke driver asserts on them).

## Tests

Unit tests (cargo always takes `-p` and `-j 6`):

```bash
cargo test -p dbt-tasks-sa -j 6 --lib full_refresh_swap
cargo test -p dbt-schemas -j 6 --lib full_refresh_swap
cargo test -p dbt-adapter-sql -j 6 --lib chunking
cargo test -p dbt-adapter -j 6 --lib swap_relations
cargo test -p dbt-loader -j 6 --test main singlestore
```

That's 10, 9, 3, 1 and 18 tests: the tag parsing and rewriting, plan validation,
hash predicates, the swap's double-failure path, and the SingleStore macros
(including the `full_refresh_swap` default/override tests). Nothing in them runs
a real rebuild, so after any behaviour change also run the end-to-end driver
below against SingleStore.

## Run it on SingleStore

**Build**: `cargo build -p dbt-sa-cli -j 6` produces `target/debug/dbt`.

**Driver.** SingleStore's ADBC driver is never downloaded: dbt looks for
`libadbc_driver_singlestore.so` in a `lib/dbt/` directory up to 5 levels above the
`dbt` executable, then in the normal `dlopen` paths (`LD_LIBRARY_PATH` works). On
this machine a driver built from `/opt/singlestore-adbc-connector` is in
`~/.cache/dbt-singlestore-base-build/out/`. Link it into the repo (`*.so` is gitignored):

```bash
mkdir -p lib/dbt && ln -sf ~/.cache/dbt-singlestore-base-build/out/libadbc_driver_singlestore.so lib/dbt/
```

Without it, dbt reports `AuthenticationFailed (dbt1011)`, with `Driver not found:
adbc_driver_singlestore` inside the message.

**Server.** The local container `singlestore` (see the user's `singlestore` skill).
Its root password is `SINGLESTORE_ROOT_PASSWORD` in `~/singlestore/.env`. Never print it
or write it to a file; profiles read it with `env_var()`. The profile's database must
already exist (else `Error 1049 ... Unknown database`), and `schema` equals `database`
on SingleStore.

**End-to-end driver**:

```bash
bash -c 'source ~/singlestore/.env; SINGLESTORE_PASSWORD="$SINGLESTORE_ROOT_PASSWORD" .agents/skills/singlestore-chunked-reloads/scripts/smoke.sh'
```

It creates the `dbt_chunk_smoke` database and builds a seeded project with an
incremental `swap` model (`chunks: 4`, chunk filters on a join) and a `table` `swap`
model chunked on `chunk_output_by`. It prints 15 PASS/FAIL checks, covering:
- chunks loaded into the shadow, and tests run on the shadow before the swap;
- exact row counts and sums;
- a plain incremental run doesn't chunk;
- the verify flag passes good filters and fails a wrong column (`dbt1423`);
- a failing test stops the swap (`dbt1422`);
- `in_place` chunks;
- no `__dbt_tmp`/`__dbt_backup` leftovers.

Then it drops the database. It takes about 15 s. `--keep` keeps the project and
the database; `--dbt PATH` drives another build.

## Configuring a model (what users write)

```yaml
models:
  - name: fct_orders
    config:
      materialized: incremental
      unique_key: order_id
      full_refresh_strategy: swap   # default in_place
      chunks: 8                     # default 1
```

Inside the model SQL, put `{{ chunk_where('<key>') }}` / `{{ chunk_and('<key>') }}` in
each import CTE that holds the grain key, on the column used in *that input's*
join. A wrong column fails nothing: the smoke driver's wrong-column run passes with
45 of 160 rows. Only `DBT_ENGINE_FEATURES=verify_chunked_rebuilds` catches it.
`docs/full-refresh-swap.md` covers the rest (when to pick `in_place`, microbatch,
operator recovery).

## Gotchas

- On SingleStore, `dbt show ... --output json -q` prints `debug:warning=static analysis
  for adapter: SingleStore is disabled` on stdout before the JSON. Take the last line.
- `dbt run --full-refresh` swaps **without** pre-swap tests: only invocations that
  select the tests (`dbt build`) run them against the shadow.
- `dbt compile`/`dbt show` of a chunked model show chunk 0's SQL only.
- The `dbt1420`/`dbt1423` messages mention `chunk_by` in `import_dependencies`.
  Neither exists in this repo: they are project-side macros, so don't search
  here for them.
- The commit message of the backfill commit cites older error numbers (dbt1416,
  dbt1417, ...). `dbt-error/src/codes.rs` and the docs are authoritative.
- The swap is two renames, so queries that land between them fail. Only one
  process may rebuild a model at a time (they would share the shadow).
