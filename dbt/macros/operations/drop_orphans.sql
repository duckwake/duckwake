{#- Views DuckWake runs that no streaming_view model of this project defines (a model deleted or renamed).
    `dbt run-operation duckwake_drop_orphans` lists them; `--args '{dry_run: false}'` stops and drops them and
    their tables, readers before the views they read. Views in `keep` (`schema.table`) are left alone, and so is an
    orphan that a view of the project still reads. -#}
{% macro duckwake_drop_orphans(dry_run=true, keep=[]) %}
  {% set defined = [] %}
  {% for n in duckwake._models('streaming_view') %}{% do defined.append(n.schema ~ '.' ~ n.alias) %}{% endfor %}
  {% set left = [] %}
  {% for job in duckwake._jobs().keys() | sort if job not in defined and job not in keep %}{% do left.append(job) %}{% endfor %}
  {% set edges = duckwake._call('deps', "SELECT DISTINCT upstream_job, job_id FROM " ~ duckwake._lake()
                                 ~ ".__duckwake.dependencies WHERE upstream_job <> job_id") %}
  {% set readers = {} %}
  {% for e in edges.rows %}{% do readers.setdefault(e[0], []).append(e[1]) %}{% endfor %}
  {% if not left %}{{ log('no orphaned views', info=True) }}{% endif %}
  {#- Each pass takes the orphans no other remaining view reads. -#}
  {% for _ in range(left | length) if left %}
    {% for job in left[:] if (readers.get(job, []) | select('in', left) | list | length) == 0 %}
      {% set held = readers.get(job, []) | select('in', defined) | list %}
      {% if held %}
        {{ log('kept: ' ~ job ~ ', read by ' ~ held | join(', '), info=True) }}
      {% elif dry_run %}
        {{ log('orphaned: ' ~ job ~ ' (dry run; --args "{dry_run: false}" drops it)', info=True) }}
      {% else %}
        {% do duckwake._call('stop', 'CALL duckwake_stop(' ~ duckwake._lit(job) ~ ')') %}
        {% do duckwake._call('drop', 'CALL duckwake_drop(' ~ duckwake._lit(job) ~ ')') %}
        {{ log('dropped: ' ~ job, info=True) }}
      {% endif %}
      {% do left.remove(job) %}
    {% endfor %}
  {% endfor %}
{% endmacro %}
