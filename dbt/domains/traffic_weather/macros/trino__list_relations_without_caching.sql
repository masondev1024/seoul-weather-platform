{#
  R2 Data Catalog returns 403 for listViews/listTables when the requested
  namespace does not exist. dbt asks for its default test-audit namespace
  (`dbt_test__audit`) even when this project has no failed tests. The default
  dbt-trino macro calls information_schema.tables immediately, so that benign
  absence aborts every serving refresh before a model starts.

  Probe the namespace list first. An absent namespace is an empty relation
  cache; catalog/network errors from the probe still fail fast and remain
  observable. Existing namespaces keep dbt-trino's materialized-view-aware
  relation listing unchanged.
#}

{% macro trino__list_relations_without_caching(relation) %}
  {% if not execute %}
    {{ return([]) }}
  {% endif %}

  {% set schema_name = relation.schema | lower | replace("'", "''") %}
  {% set schema_probe_sql %}
    select schema_name
    from {{ relation.information_schema() }}.schemata
    where schema_name = '{{ schema_name }}'
    limit 1
  {% endset %}
  {% set schema_probe = run_query(schema_probe_sql) %}
  {% if schema_probe is none or (schema_probe | length) == 0 %}
    {% do log(
      "Trino Iceberg namespace is absent; skip table/view listing: "
      ~ relation.database ~ "." ~ schema_name,
      info=False
    ) %}
    {{ return([]) }}
  {% endif %}

  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      t.table_catalog as database,
      t.table_name as name,
      t.table_schema as schema,
      case when mv.name is not null then 'materialized_view'
           when t.table_type = 'BASE TABLE' then 'table'
           when t.table_type = 'VIEW' then 'view'
           else t.table_type
      end as table_type
    from {{ relation.information_schema() }}.tables t
    left join (
            select * from system.metadata.materialized_views
            where catalog_name = '{{ relation.database | lower }}'
              and schema_name = '{{ schema_name }}') mv
          on mv.catalog_name = t.table_catalog and mv.schema_name = t.table_schema and mv.name = t.table_name
    where t.table_schema = '{{ schema_name }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}
