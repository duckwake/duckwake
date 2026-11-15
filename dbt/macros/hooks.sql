{#- For on-run-start: points the DuckWake extension at the lake, from the vars duckwake_catalog and
    duckwake_data_path or the DUCKWAKE_CATALOG and DUCKWAKE_DATA_PATH environment (which the extension also
    reads by itself when neither is set). -#}
{% macro duckwake_setup() -%}
  {%- set catalog = var('duckwake_catalog', env_var('DUCKWAKE_CATALOG', '')) -%}
  {%- set data_path = var('duckwake_data_path', env_var('DUCKWAKE_DATA_PATH', '')) -%}
  {%- if catalog -%}
    CALL duckwake_connect({{ duckwake._lit('ducklake:' ~ (catalog | replace('ducklake:', ''))) }}
      {%- if data_path %}, data_path := {{ duckwake._lit(data_path) }}{% endif %})
  {%- else -%}
    SELECT 1
  {%- endif -%}
{%- endmacro %}

{#- For on-run-end: with --vars '{duckwake_wait: true}', waits until every streaming view this run built has
    caught up with its inputs (duckwake_wait, up to duckwake_wait_s seconds each, default 300), so that
    `dbt test` right after sees their output. -#}
{% macro duckwake_wait_built(results) -%}
  {%- if var('duckwake_wait', false) and execute -%}
    {%- for r in results if r.node.resource_type == 'model' and r.node.config.materialized == 'streaming_view'
                            and r.status == 'success' -%}
      {%- set name = r.node.schema ~ '.' ~ r.node.alias -%}
      {% set rows = duckwake._call('wait', 'CALL duckwake_wait(' ~ duckwake._lit(name) ~ ', timeout_s := '
                                    ~ var('duckwake_wait_s', 300) ~ ')') %}
      {{ log(rows.rows[0][1], info=True) }}
    {%- endfor -%}
  {%- endif -%}
  SELECT 1
{%- endmacro %}
