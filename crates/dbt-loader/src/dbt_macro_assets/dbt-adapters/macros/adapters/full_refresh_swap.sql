{#
  Override points for `full_refresh_strategy: swap`. The rebuild itself is
  orchestrated by dbt; these macros only produce the SQL that differs per
  warehouse or per project. Override them like any other dispatched macro:
  define `<adapter>__get_chunk_predicate` / `<adapter>__swap_relations`, or
  `default__...` in your root project (see docs/full-refresh-swap.md).
#}

-- funcsign: (string, integer, integer) -> string
{% macro get_chunk_predicate(column, chunks, chunk) -%}
  {{ return(adapter.dispatch('get_chunk_predicate', 'dbt')(column, chunks, chunk)) }}
{%- endmacro %}

-- funcsign: (string, integer, integer) -> string
{% macro default__get_chunk_predicate(column, chunks, chunk) -%}
  {{ return(adapter.chunk_predicate(column, chunks, chunk)) }}
{%- endmacro %}

{#
  A chunk predicate to put inside the model SQL, typically in the `where` of an
  import CTE, so each chunk of a rebuild only reads its share of that input.
  Most models use `chunk_where` / `chunk_and` below instead.

  Returns none unless the model is rebuilt in chunks (`chunks` > 1 on a table
  model, or on an incremental model outside incremental mode). Then it returns
  the predicate for chunk 0, tagged `/* dbt_chunk: <column> */`; dbt rewrites
  it for every other chunk and does not filter the model's output. Use the same
  key everywhere in a model, so rows that are joined together share a chunk.
#}
-- funcsign: (string) -> optional[string]
{% macro get_chunk_filter(column) -%}
  {%- set chunks = config.get('chunks') or 1 -%}
  {%- if chunks > 1
        and config.get('materialized') in ['table', 'incremental']
        and not is_incremental() -%}
    {{ return(get_chunk_predicate(column, chunks, 0) ~ ' /* dbt_chunk: ' ~ column ~ ' */') }}
  {%- endif -%}
  {{ return(none) }}
{%- endmacro %}

{#
  `where <chunk predicate>` during a chunked rebuild, and nothing otherwise:

    select * from {{ ref('orders') }}
    {{ chunk_where('customer_id') }}
#}
-- funcsign: (string) -> string
{% macro chunk_where(column) -%}
  {%- set filter = get_chunk_filter(column) -%}
  {{ return('where ' ~ filter if filter else '') }}
{%- endmacro %}

{#
  `and <chunk predicate>` during a chunked rebuild, and nothing otherwise, for a
  query that already has a `where`:

    select * from {{ ref('orders') }}
    where status != 'cancelled'
    {{ chunk_and('customer_id') }}
#}
-- funcsign: (string) -> string
{% macro chunk_and(column) -%}
  {%- set filter = get_chunk_filter(column) -%}
  {{ return('and ' ~ filter if filter else '') }}
{%- endmacro %}

{#
  A query returning one row `(row_count, row_hash_sum)` that describes the rows
  of `sql` in any order: the number of rows and the sum of a hash of each row.
  Fingerprints of disjoint row sets add up. Chunked rebuilds compare them to
  verify their chunk filters (`chunk_verification`).

  `columns` are the target relation's columns (`adapter.get_columns_in_relation`),
  hashed in that order. Values are hashed as `sql` returns them, so model SQL and
  the relation it was loaded into hash alike as long as the relation's column
  types are the ones the SQL produces (they are when the first chunk creates it).

  The default hashes each row the way `dbt_utils.generate_surrogate_key` builds
  its key: every value cast to a string, NULL replaced by a marker, joined with
  a separator; then `adapter.row_hash`, a non-negative 32-bit hash.
#}
-- funcsign: (string, list[base_column]) -> string
{% macro get_rows_fingerprint(sql, columns) -%}
  {{ return(adapter.dispatch('get_rows_fingerprint', 'dbt')(sql, columns)) }}
{%- endmacro %}

-- funcsign: (string, list[base_column]) -> string
{% macro default__get_rows_fingerprint(sql, columns) -%}
  {%- set fields = [] -%}
  {%- for column in columns -%}
    {%- do fields.append("coalesce(" ~ cast(column.quoted, type_string()) ~ ", '_dbt_null_')") -%}
    {%- if not loop.last -%}{%- do fields.append("'|'") -%}{%- endif -%}
  {%- endfor -%}
  {%- set row_hash = adapter.row_hash(concat(fields)) -%}
  {{ return(
      "select count(*) as row_count, coalesce(sum(" ~ cast("(" ~ row_hash ~ ")", type_numeric())
      ~ "), 0) as row_hash_sum from (\n" ~ sql ~ "\n) as dbt_fingerprint_source"
  ) }}
{%- endmacro %}

-- funcsign: (relation, relation, relation) -> none
{% macro swap_relations(relation, shadow, backup) -%}
  {{ return(adapter.dispatch('swap_relations', 'dbt')(relation, shadow, backup)) }}
{%- endmacro %}

-- funcsign: (relation, relation, relation) -> none
{% macro default__swap_relations(relation, shadow, backup) -%}
  {% do adapter.swap_relations(relation, shadow, backup) %}
{%- endmacro %}
