{#- A view's output equals its query run as one batch over everything it has read (duckwake_verify). Fails
    with the verify report as the failing row. Run it once the view has committed (duckwake_wait). -#}
{% test duckwake_verified(model) %}
  SELECT * FROM duckwake_verify({{ duckwake._lit(duckwake._view_name(model)) }})
  WHERE result NOT LIKE 'VERIFY OK%'
{% endtest %}
