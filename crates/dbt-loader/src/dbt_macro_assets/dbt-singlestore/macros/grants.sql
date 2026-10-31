{% macro singlestore__get_show_grant_sql(relation) %}
  {%- set target_schema = relation.database | default(relation.schema, true) | trim -%}
  {%- if not target_schema -%}
    {%- set target_schema = relation.schema | trim -%}
  {%- endif -%}
  -- table_privileges stores user privileges for both tables and views.
  -- Usernames are formatted as 'user'@'host'.
  select privilege_type,
         case
           when locate('@', grantee) > 0 then
             substring(grantee, 2, length(grantee) - length(substring_index(grantee, '@', -1)) - 3)
           else
             replace(grantee, '`', '')
         end as grantee
    from information_schema.table_privileges
   where table_schema = '{{ escape_single_quotes(target_schema) }}'
     and table_name = '{{ escape_single_quotes(relation.identifier) }}'

  union all

  -- role_privileges stores role privileges for both tables and views
  select privileges as privilege_type,
         concat('ROLE ', role) as grantee
    from information_schema.role_privileges
   where `database` = '{{ escape_single_quotes(target_schema) }}'
     and `table` = '{{ escape_single_quotes(relation.identifier) }}'
{% endmacro %}

{% macro singlestore__format_grantee(grantee) %}
  {%- if grantee.upper().startswith('ROLE ') -%}
    {{ return(grantee) }}
  {%- elif grantee.startswith("'") or grantee.startswith("`") -%}
    {{ return(grantee) }}
  {%- else -%}
    {{ return("'" ~ grantee ~ "'") }}
  {%- endif -%}
{% endmacro %}

{% macro singlestore__get_grant_sql(relation, privilege, grantees) %}
  {%- set formatted_grantees = [] -%}
  {%- for g in grantees -%}
    {%- do formatted_grantees.append(singlestore__format_grantee(g)) -%}
  {%- endfor -%}
  grant {{ privilege }} on {{ relation.render() }} to {{ formatted_grantees | join(', ') }}
{% endmacro %}

{% macro singlestore__get_revoke_sql(relation, privilege, grantees) %}
  {%- set formatted_grantees = [] -%}
  {%- for g in grantees -%}
    {%- do formatted_grantees.append(singlestore__format_grantee(g)) -%}
  {%- endfor -%}
  revoke {{ privilege }} on {{ relation.render() }} from {{ formatted_grantees | join(', ') }}
{% endmacro %}

{% macro singlestore__call_dcl_statements(dcl_statement_list) %}
    {% for dcl_statement in dcl_statement_list %}
        {% call statement('grants') %}
            {{ dcl_statement }}
        {% endcall %}
    {% endfor %}
{% endmacro %}

{% macro singlestore__support_multiple_grantees_per_dcl_statement() %}
    {{ return(False) }}
{% endmacro %}

-- funcsign: (relation, optional[dict[string, list[string]]], bool) -> string
{% macro singlestore__apply_grants(relation, grant_config, should_revoke=True) %}
    {#-- If grant_config is None (unset), grants are unmanaged by dbt on this model.
         Unmanaged grants made outside dbt are preserved (no-op).
         If grant_config is {} (explicitly empty mapping), all existing grants should be revoked. --#}
    {% if grant_config is not none %}
        {% if should_revoke %}
            {#-- We think previous grants may have carried over --#}
            {#-- Show current grants and calculate diffs --#}
            {% set current_grants_table = run_query(get_show_grant_sql(relation)) %}
            {% set current_grants_dict = adapter.standardize_grants_dict(current_grants_table) %}
            {% set needs_granting = diff_of_two_dicts(grant_config, current_grants_dict) %}
            {% set needs_revoking = diff_of_two_dicts(current_grants_dict, grant_config) %}
            {% if not (needs_granting or needs_revoking) %}
                {{ log('On ' ~ relation.render() ~': All grants are in place, no revocation or granting needed.')}}
            {% endif %}
        {% else %}
            {#-- We don't think there's any chance of previous grants having carried over. --#}
            {#-- Jump straight to granting what the user has configured. --#}
            {% set needs_revoking = {} %}
            {% set needs_granting = grant_config %}
        {% endif %}
        {% if needs_granting or needs_revoking %}
            {% set revoke_statement_list = get_dcl_statement_list(relation, needs_revoking, get_revoke_sql) %}
            {% set grant_statement_list = get_dcl_statement_list(relation, needs_granting, get_grant_sql) %}
            {% set dcl_statement_list = revoke_statement_list + grant_statement_list %}
            {% if dcl_statement_list %}
                {{ call_dcl_statements(dcl_statement_list) }}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}
