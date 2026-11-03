{#
  SingleStore snapshot materialization support.

  SingleStore does not have a MERGE statement, so the default dbt snapshot
  materialization dispatches to `singlestore__snapshot_merge_sql` which
  implements the merge semantics using two statements:
    1. UPDATE ... JOIN ... SET dbt_valid_to = ...
    2. INSERT INTO ... SELECT ... WHERE dbt_change_type = 'insert'

  The default `materialization snapshot` (from dbt-adapters) works out of the
  box for SingleStore because it calls `snapshot_merge_sql`, which dispatches
  to our `singlestore__snapshot_merge_sql` override below.

  What this file provides:
  - `singlestore__snapshot_hash_arguments`: uses MD5() for SCD id generation
  - `singlestore__post_snapshot`:           drops the staging table after use
#}

{# Use SingleStore's built-in MD5() for snapshot SCD hash generation.
   `||` is logical OR in SingleStore (as in MySQL), so join with concat(). #}
{% macro singlestore__snapshot_hash_arguments(args) -%}
  md5(concat({%- for arg in args -%}
    coalesce(cast({{ arg }} as char), '')
    {% if not loop.last %}, '|', {% endif %}
  {%- endfor -%}))
{%- endmacro %}

{# Drop the temporary staging table created during snapshot execution #}
{% macro singlestore__post_snapshot(staging_relation) %}
    {{ drop_relation_if_exists(staging_relation) }}
{% endmacro %}

{# SingleStore CTAS infers untyped null or string literals in coalesce() as VARCHAR.
   Cast to datetime so dbt_valid_to is created as DATETIME matching upstream dbt-singlestore:
   https://github.com/memsql/dbt-singlestore/blob/master/dbt/include/singlestore/macros/materializations/snapshot/helpers.sql#L250-L277 #}
{% macro singlestore__get_dbt_valid_to_current(strategy, columns) %}
  {% set dbt_valid_to_current = config.get('dbt_valid_to_current') or "null" %}
  (coalesce(
    nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}),
    ({{ dbt_valid_to_current }}) :> datetime)
    :> datetime)
  as {{ columns.dbt_valid_to }}
{% endmacro %}

{% macro singlestore__build_snapshot_table(strategy, sql) %}
    {% set columns = config.get('snapshot_table_column_names') or get_snapshot_table_column_names() %}

    select *,
        {{ strategy.scd_id }} as {{ columns.dbt_scd_id }},
        {{ strategy.updated_at }} as {{ columns.dbt_updated_at }},
        {{ strategy.updated_at }} as {{ columns.dbt_valid_from }},
        {{ get_dbt_valid_to_current(strategy, columns) }}
      {%- if strategy.hard_deletes == 'new_record' -%}
        , 'False' as {{ columns.dbt_is_deleted }}
      {% endif -%}
    from (
        {{ sql }}
    ) sbq

{% endmacro %}
