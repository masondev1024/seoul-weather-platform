{% macro weather_quality_required_date_value(name) -%}
  {%- set value = var(name, '') | string | trim -%}
  {%- if not modules.re.fullmatch('^[0-9]{4}-[0-9]{2}-[0-9]{2}$', value) -%}
    {{ exceptions.raise_compiler_error(name ~ ' must be an ISO date') }}
  {%- endif -%}
  {{ return(value) }}
{%- endmacro %}

{% macro weather_quality_required_date(name) -%}
  {%- set value = weather_quality_required_date_value(name) -%}
  cast('{{ value }}' as date)
{%- endmacro %}

{% macro weather_quality_required_timestamp_value(name) -%}
  {%- set value = var(name, '') | string | trim -%}
  {%- set iso_timestamp_re = '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$' -%}
  {%- if not modules.re.fullmatch(iso_timestamp_re, value) -%}
    {{ exceptions.raise_compiler_error(name ~ ' must be an ISO timestamp with timezone') }}
  {%- endif -%}
  {{ return(value) }}
{%- endmacro %}

{% macro weather_quality_required_timestamp(name) -%}
  {%- set value = weather_quality_required_timestamp_value(name) -%}
  from_iso8601_timestamp('{{ value }}')
{%- endmacro %}

{% macro weather_quality_required_run_id_value(name) -%}
  {%- set value = var(name, '') | string -%}
  {%- if value != value | trim or not modules.re.fullmatch('^[A-Za-z0-9._=:+-]+$', value) -%}
    {{ exceptions.raise_compiler_error(name ~ ' must be a safe run ID') }}
  {%- endif -%}
  {{ return(value) }}
{%- endmacro %}

{% macro weather_quality_required_run_id(name) -%}
  {%- set value = weather_quality_required_run_id_value(name) -%}
  '{{ value }}'
{%- endmacro %}

{% macro weather_quality_required_policy_version_value(name, expected) -%}
  {%- set value = var(name, '') | string | trim -%}
  {%- if value != expected -%}
    {{ exceptions.raise_compiler_error(name ~ ' must be ' ~ expected) }}
  {%- endif -%}
  {{ return(value) }}
{%- endmacro %}

{% macro weather_quality_required_policy_version(name, expected) -%}
  {%- set value = weather_quality_required_policy_version_value(name, expected) -%}
  '{{ value }}'
{%- endmacro %}

{% macro weather_quality_validate_runtime_contract() -%}
  {%- set run_id = weather_quality_required_run_id_value('weather_quality_run_id') -%}
  {%- set evaluation_as_of = weather_quality_required_timestamp_value('weather_quality_evaluation_as_of') -%}
  {%- set window_start = weather_quality_required_date_value('weather_quality_window_start_date') -%}
  {%- set window_end = weather_quality_required_date_value('weather_quality_window_end_date') -%}
  {%- set forecast_load_start = weather_quality_required_date_value('weather_quality_forecast_load_start_date') -%}
  {%- set forecast_load_end = weather_quality_required_date_value('weather_quality_forecast_load_end_date') -%}
  {%- set truth_policy = weather_quality_required_policy_version_value('weather_quality_truth_policy_version', 'observation-truth-policy/v2-internal') -%}
  {%- set vintage_policy = weather_quality_required_policy_version_value('weather_quality_vintage_policy_version', 'forecast-vintage-cutoff/v1') -%}
  {%- set evidence_policy = weather_quality_required_policy_version_value('weather_quality_evidence_policy_version', 'metric-evidence-gate/v1') -%}
  {%- set pop_policy = weather_quality_required_policy_version_value('weather_quality_pop_policy_version', 'pop-threshold-0.5/v1') -%}
  {%- if window_start > window_end -%}
    {{ exceptions.raise_compiler_error('weather_quality_window_start_date must be <= weather_quality_window_end_date') }}
  {%- endif -%}
  {%- set window_start_date = modules.datetime.date.fromisoformat(window_start) -%}
  {%- set window_end_date = modules.datetime.date.fromisoformat(window_end) -%}
  {%- set window_day_count = (window_end_date - window_start_date).days + 1 -%}
  {%- if window_day_count not in [1, 7] -%}
    {{ exceptions.raise_compiler_error('weather_quality_window_date span must be exactly 1 or 7 KST dates') }}
  {%- endif -%}
  {%- set evaluation_text = evaluation_as_of | replace('Z', '+00:00') -%}
  {%- set evaluation_offset_text = evaluation_text[-6:] -%}
  {%- set evaluation_local_text = evaluation_text[:-6] -%}
  {%- set evaluation_local_datetime = modules.datetime.datetime.fromisoformat(evaluation_local_text) -%}
  {%- set evaluation_offset_sign = 1 if evaluation_offset_text[0] == '+' else -1 -%}
  {%- set evaluation_offset_hours = evaluation_offset_text[1:3] | int -%}
  {%- set evaluation_offset_minutes = evaluation_offset_text[4:6] | int -%}
  {%- set evaluation_utc_datetime = evaluation_local_datetime - (
        modules.datetime.timedelta(
          hours=evaluation_offset_hours,
          minutes=evaluation_offset_minutes
        ) * evaluation_offset_sign
      ) -%}
  {%- set evaluation_kst_date = (
        evaluation_utc_datetime + modules.datetime.timedelta(hours=9)
      ).date() -%}
  {%- if window_end_date >= evaluation_kst_date -%}
    {{ exceptions.raise_compiler_error('weather_quality_window_end_date must be before the evaluation KST date') }}
  {%- endif -%}
  {%- if forecast_load_start > forecast_load_end -%}
    {{ exceptions.raise_compiler_error('weather_quality_forecast_load_start_date must be <= weather_quality_forecast_load_end_date') }}
  {%- endif -%}
  {{ return(true) }}
{%- endmacro %}

{% macro weather_quality_run_id() -%}
  {{ weather_quality_required_run_id('weather_quality_run_id') }}
{%- endmacro %}

{% macro weather_quality_evaluation_as_of() -%}
  {{ weather_quality_required_timestamp('weather_quality_evaluation_as_of') }}
{%- endmacro %}

{% macro weather_quality_window_start_date() -%}
  {{ weather_quality_required_date('weather_quality_window_start_date') }}
{%- endmacro %}

{% macro weather_quality_window_end_date() -%}
  {{ weather_quality_required_date('weather_quality_window_end_date') }}
{%- endmacro %}

{% macro weather_quality_forecast_load_start_date() -%}
  {{ weather_quality_required_date('weather_quality_forecast_load_start_date') }}
{%- endmacro %}

{% macro weather_quality_forecast_load_end_date() -%}
  {{ weather_quality_required_date('weather_quality_forecast_load_end_date') }}
{%- endmacro %}

{% macro weather_quality_truth_policy_version() -%}
  {{ weather_quality_required_policy_version('weather_quality_truth_policy_version', 'observation-truth-policy/v2-internal') }}
{%- endmacro %}

{% macro weather_quality_vintage_policy_version() -%}
  {{ weather_quality_required_policy_version('weather_quality_vintage_policy_version', 'forecast-vintage-cutoff/v1') }}
{%- endmacro %}

{% macro weather_quality_evidence_policy_version() -%}
  {{ weather_quality_required_policy_version('weather_quality_evidence_policy_version', 'metric-evidence-gate/v1') }}
{%- endmacro %}

{% macro weather_quality_pop_policy_version() -%}
  {{ weather_quality_required_policy_version('weather_quality_pop_policy_version', 'pop-threshold-0.5/v1') }}
{%- endmacro %}

{% macro weather_quality_evidence_state(sample_count, expected_count, provisional) -%}
case
  when {{ sample_count }} < 30
    or cast({{ sample_count }} as double) / nullif({{ expected_count }}, 0) < 0.80
    then 'insufficient_evidence'
  when {{ provisional }} then 'degraded'
  else 'sufficient'
end
{%- endmacro %}
