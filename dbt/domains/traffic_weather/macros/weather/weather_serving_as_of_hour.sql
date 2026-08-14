{% macro weather_serving_as_of_hour() %}
  {%- set raw = var('weather_serving_as_of_hour', none) -%}
  {%- set value = raw | string | trim if raw is not none else '' -%}
  {%- if not value -%}
    date_trunc('hour', cast(current_timestamp at time zone 'Asia/Seoul' as timestamp(6)))
  {%- else -%}
    {%- if not modules.re.fullmatch('^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:00:00$', value) -%}
      {{ exceptions.raise_compiler_error(
        'weather_serving_as_of_hour must be a KST hour formatted YYYY-MM-DD HH:00:00.'
      ) }}
    {%- endif -%}
    cast('{{ value }}' as timestamp(6))
  {%- endif -%}
{% endmacro %}
