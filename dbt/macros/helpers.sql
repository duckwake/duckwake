{#- A SQL string literal. -#}
{% macro _lit(value) -%}
  '{{ (value | string) | replace("'", "''") }}'
{%- endmacro %}

{#- The DuckWake name of a model's view: the lake table it maintains, `schema.table`. -#}
{% macro _view_name(relation) -%}
  {{ relation.schema }}.{{ relation.identifier }}
{%- endmacro %}

{#- The database the lake is attached as in the dbt session, which holds DuckWake's control tables. -#}
{% macro _lake() -%}
  {{ var('duckwake_lake', 'lake') }}
{%- endmacro %}

{#- Runs one DuckWake call and returns its rows (a list of dicts); the call's error fails the model. -#}
{% macro _call(name, sql) %}
  {% call statement(name, fetch_result=True, auto_begin=False) %}{{ sql }}{% endcall %}
  {{ return(load_result(name).table) }}
{% endmacro %}

{#- Every view DuckWake has, as a dict from job id (the table name) to status. -#}
{% macro _jobs() %}
  {% set rows = duckwake._call('duckwake_jobs', 'SELECT job_id, status FROM duckwake_jobs()') %}
  {% set out = {} %}
  {% for r in rows.rows %}{% do out.update({r[0]: r[1]}) %}{% endfor %}
  {{ return(out) }}
{% endmacro %}

{#- The model nodes of materialization `kind`. -#}
{% macro _models(kind) %}
  {% set out = [] %}
  {% for n in graph.nodes.values() if n.resource_type == 'model' and n.config.materialized == kind %}
    {% do out.append(n) %}
  {% endfor %}
  {{ return(out) }}
{% endmacro %}

{#- A model's `SELECT` with every ref() to a streaming_source model replaced by the source's bare name:
    DuckWake reads a Kafka source by its name, while ref() renders it as a lake relation. ref() to a view
    renders as its lake table ("lake"."analytics"."x"), which DuckWake reads as is. -#}
{% macro _source_names(model, sql) %}
  {% set ns = namespace(out=sql) %}
  {% for uid in model.depends_on.nodes %}
    {% set n = graph.nodes.get(uid) %}
    {% if n and n.config.materialized == 'streaming_source' %}
      {% set rendered = api.Relation.create(database=n.database, schema=n.schema, identifier=n.alias) | string %}
      {% set ns.out = ns.out | replace(rendered, '"' ~ n.alias ~ '"') %}
    {% endif %}
  {% endfor %}
  {{ return(ns.out) }}
{% endmacro %}
