{#
  A Kafka source, from config alone (the model file holds only config()). Created on the first run; later runs
  add the columns the model gained (duckwake_add_source_column) and fail on any other change, which needs a
  source under a new name: views that read a source keep the options they were made with.

  Config: brokers, topic, event_time (not needed with format='debezium'), columns (a mapping of name to DuckDB
  type, in order), and optionally format, watermark_delay, idle_timeout, start, decode, security_protocol, sasl_mechanism, sasl_username,
  sasl_password ('env:NAME' or 'file:/path', never the password itself), ssl_ca_location,
  ssl_certificate_location, ssl_key_location.
#}
{% materialization streaming_source, adapter='duckdb' %}
  {%- set name = this.identifier -%}
  {%- set keys = ['brokers', 'topic', 'format', 'event_time', 'watermark_delay', 'idle_timeout', 'start', 'decode',
                  'security_protocol', 'sasl_mechanism', 'sasl_username', 'sasl_password', 'ssl_ca_location',
                  'ssl_certificate_location', 'ssl_key_location'] -%}
  {%- set options = {} -%}
  {%- for k in keys if config.get(k) is not none -%}{% do options.update({k: config.get(k) | string}) %}{%- endfor -%}
  {%- set columns = config.get('columns') or {} -%}
  {%- if not columns -%}{{ exceptions.raise_compiler_error("source " ~ name ~ ": config needs columns") }}{%- endif -%}

  {{ run_hooks(pre_hooks) }}

  {%- set stored = duckwake._call('stored', "SELECT options::VARCHAR FROM " ~ duckwake._lake() ~ ".__duckwake.sources WHERE name = "
                                    ~ duckwake._lit(name)) -%}
  {%- if stored.rows | length == 0 -%}
    {%- set json = options.copy() -%}
    {% do json.update({'columns': columns}) %}
    {%- set rows = duckwake._call('main', 'CALL duckwake_create_source(' ~ duckwake._lit(name) ~ ", 'kafka', "
                                    ~ duckwake._lit(tojson(json)) ~ ')') -%}
    {{ log(name ~ ': created, ' ~ rows.rows[0][1], info=True) }}
  {%- else -%}
    {%- set have = fromjson(stored.rows[0][0]) -%}
    {%- set changed = [] -%}
    {%- for k, v in options.items() if (have.get(k) | string) != v -%}
      {% do changed.append(k ~ ' changed (' ~ have.get(k) ~ ' -> ' ~ v ~ ')') %}
    {%- endfor -%}
    {%- set types = {} -%}
    {%- for c in have['columns'] -%}{% do types.update({c[0] | lower: c[1]}) %}{%- endfor -%}
    {%- set added = [] -%}
    {%- for c, ty in columns.items() -%}
      {%- if c | lower not in types -%}{% do added.append((c, ty)) %}
      {%- elif (types[c | lower] | upper | replace(' ', '')) != (ty | upper | replace(' ', '')) -%}
        {% do changed.append('column ' ~ c ~ ' changed type (' ~ types[c | lower] ~ ' -> ' ~ ty ~ ')') %}
      {%- endif -%}
    {%- endfor -%}
    {%- set wanted = columns.keys() | map('lower') | list -%}
    {%- for c in types.keys() if c not in wanted -%}{% do changed.append('column ' ~ c ~ ' removed') %}{%- endfor -%}
    {%- if changed -%}
      {{ exceptions.raise_compiler_error("source " ~ name ~ ": " ~ changed | join(', ') ~ ". A source can only gain columns: "
         ~ "create the changed one under a new name and point its views at it (dbt run --full-refresh for the views).") }}
    {%- endif -%}
    {%- for c, ty in added -%}
      {% do duckwake._call('add', 'CALL duckwake_add_source_column(' ~ duckwake._lit(name) ~ ', ' ~ duckwake._lit(c) ~ ', '
                            ~ duckwake._lit(ty) ~ ')') %}
      {{ log(name ~ ': added column ' ~ c ~ ' ' ~ ty, info=True) }}
    {%- endfor -%}
    {%- if not added -%}{{ log(name ~ ': unchanged', info=True) }}{%- endif -%}
    {#- dbt wants a 'main' statement in every materialization. -#}
    {% call statement('main', auto_begin=False) %}SELECT 1{% endcall %}
  {%- endif -%}

  {{ run_hooks(post_hooks) }}
  {{ return({'relations': []}) }}
{% endmaterialization %}
