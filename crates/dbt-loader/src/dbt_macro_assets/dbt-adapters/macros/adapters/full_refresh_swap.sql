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

-- funcsign: (relation, relation, relation) -> none
{% macro swap_relations(relation, shadow, backup) -%}
  {{ return(adapter.dispatch('swap_relations', 'dbt')(relation, shadow, backup)) }}
{%- endmacro %}

-- funcsign: (relation, relation, relation) -> none
{% macro default__swap_relations(relation, shadow, backup) -%}
  {% do adapter.swap_relations(relation, shadow, backup) %}
{%- endmacro %}
