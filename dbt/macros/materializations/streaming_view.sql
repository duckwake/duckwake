{#
  A streaming view: the model's SELECT becomes duckwake_create_view(...). A plain run never recomputes a view:
  unchanged does nothing, changed settings are altered in place, and a changed query fails until
  --full-refresh, which replaces the view (it recomputes from its inputs). A view the run creates is started
  (on any worker, or the `worker` config).

  Config: epoch_interval, max_rows, late_data, checkpoint, inline_row_limit, max_state_rows, max_late_rows,
  start ('earliest' or 'latest', for views over lake tables and views), worker, start_on_create (default true).
#}
{% materialization streaming_view, adapter='duckdb' %}
  {%- set name = duckwake._view_name(this) -%}
  {%- set full = should_full_refresh() -%}
  {%- set query = duckwake._source_names(model, sql) -%}

  {{ run_hooks(pre_hooks) }}

  {#- A view other views read cannot be replaced: on --full-refresh its readers must be selected too, and are
      dropped here (deepest first) to be created again after it, in dbt's order. -#}
  {%- if full -%}
    {% do duckwake._drop_selected_readers(model) %}
  {%- endif -%}

  {%- set args = [duckwake._lit(name), duckwake._lit(query)] -%}
  {%- for key in ['epoch_interval', 'late_data', 'checkpoint', 'start'] -%}
    {%- if config.get(key) is not none -%}{% do args.append(key ~ ' := ' ~ duckwake._lit(config.get(key))) %}{%- endif -%}
  {%- endfor -%}
  {%- for key in ['max_rows', 'inline_row_limit', 'max_state_rows', 'max_late_rows'] -%}
    {%- if config.get(key) is not none -%}{% do args.append(key ~ ' := ' ~ (config.get(key) | int)) %}{%- endif -%}
  {%- endfor -%}
  {% do args.append("if_exists := " ~ duckwake._lit('replace' if full else 'update')) %}

  {%- set rows = duckwake._call('main', 'CALL duckwake_create_view(' ~ args | join(', ') ~ ')') -%}
  {%- set result = rows.rows[0][1] -%}
  {%- set outcome = result.split(' ')[0] -%}
  {{ log(name ~ ': ' ~ result, info=True) }}

  {%- if outcome == 'created' and config.get('start_on_create', true) -%}
    {%- set worker = config.get('worker', 'any') -%}
    {% do duckwake._call('start', 'CALL duckwake_start(' ~ duckwake._lit(name) ~ ', worker := ' ~ duckwake._lit(worker) ~ ')') %}
  {%- endif -%}

  {{ run_hooks(post_hooks) }}
  {{ return({'relations': [this]}) }}
{% endmaterialization %}

{#- The streaming views that read `model`, directly or through other views, deepest first. -#}
{% macro _readers(model) %}
  {% set out = [] %}
  {% for n in duckwake._models('streaming_view') if model.unique_id in n.depends_on.nodes %}
    {% for deeper in duckwake._readers(n) if deeper not in out %}{% do out.append(deeper) %}{% endfor %}
    {% if n not in out %}{% do out.append(n) %}{% endif %}
  {% endfor %}
  {{ return(out) }}
{% endmacro %}

{% macro _drop_selected_readers(model) %}
  {% set readers = duckwake._readers(model) %}
  {% set missing = [] %}
  {% for r in readers if r.unique_id not in selected_resources %}{% do missing.append(r.name) %}{% endfor %}
  {% if missing %}
    {{ exceptions.raise_compiler_error(
        "view " ~ model.name ~ " is read by " ~ missing | join(', ') ~ ", which a replace would leave reading a view "
        ~ "that starts over: select them too, e.g. dbt run -s " ~ model.name ~ "+ --full-refresh") }}
  {% endif %}
  {% set jobs = duckwake._jobs() %}
  {% for r in readers %}
    {% set rel = api.Relation.create(database=r.database, schema=r.schema, identifier=r.alias) %}
    {% if rel.identifier in jobs %}
      {% set name = duckwake._view_name(rel) %}
      {{ log(name ~ ': dropped, to be created again after ' ~ model.name, info=True) }}
      {% do duckwake._call('stop', 'CALL duckwake_stop(' ~ duckwake._lit(name) ~ ')') %}
      {% do duckwake._call('drop', 'CALL duckwake_drop(' ~ duckwake._lit(name) ~ ')') %}
    {% endif %}
  {% endfor %}
{% endmacro %}
