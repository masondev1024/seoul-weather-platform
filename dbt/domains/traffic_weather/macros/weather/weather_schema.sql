{% macro weather_schema_name() -%}
    {{ return(env_var('WEATHER_SCHEMA', 'weather')) }}
{%- endmacro %}
