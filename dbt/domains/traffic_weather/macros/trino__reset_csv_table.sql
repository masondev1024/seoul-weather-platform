{# Trino seed reset override — 재시딩 시 Iceberg snapshot 히스토리를 보존한다.

   문제: dbt-trino 기본 `trino__reset_csv_table`은 full-refresh 여부와 무관하게 항상
   `drop_relation` + `create_csv_table`(DROP -> CREATE)로 테이블을 재생성한다. 그래서
   `dbt seed`(--full-refresh 없이)마다 seoul_admin_dong_crosswalk의 이전 Iceberg
   snapshot이 통째로 사라지고, gold의 crosswalk pin(FOR VERSION AS OF <snapshot_id>,
   ASAC-DAG #480)이 dangling 되어 "Iceberg snapshot ID does not exists"로 상시 실패했다.

   왜 root 프로젝트인가: 이 macro는 `adapter.dispatch('reset_csv_table', 'dbt')`로 해석되며
   root 프로젝트 정의가 dbt-trino 기본보다 우선한다. (참고: seed materialization 자체를
   override하는 방식은 dbt가 "설치된 package"의 materialization override를 무시하기 때문에
   packages/asac_axes에 두면 동작하지 않았다 — dispatch되는 이 단일 macro가 올바른
   확장점이다. dbt-trino의 create/load(타입 캐스팅 포함) 로직은 그대로 재사용한다.)

   동작:
   - `--full-refresh`(명시적 재구성, 스키마 변경 허용): 기본과 동일하게 DROP -> CREATE.
     히스토리 손실을 감수한다(이후 다음 run의 pin이 새 snapshot을 선택).
   - 일반 재시딩(--full-refresh 없음, DAG의 `dbt seed` 경로): 같은 테이블에 DELETE(전체).
     Trino는 TRUNCATE를 지원하지 않으므로 DELETE를 쓴다. delete/insert snapshot이 같은
     Iceberg 테이블에 append되어 이전 snapshot이 유지되고, 재시딩 후에도
     FOR VERSION AS OF <pinned>가 resolve된다. 테이블을 재생성하지 않으므로 스키마가
     동일해야 한다(CSV 스키마 변경 시에는 --full-refresh로 재구성).

   적용 범위: 이 dbt 프로젝트(traffic_weather)의 seed에만 적용된다(asac_axes 공용 seed +
   weather seed). 다른 도메인 dbt 프로젝트(culture/commerce/citydata/transit)는 각자의
   materialization 해석을 쓰므로 영향받지 않는다. #}

{% macro trino__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
  {%- if full_refresh -%}
    {{ adapter.drop_relation(old_relation) }}
    {{ return(create_csv_table(model, agate_table)) }}
  {%- else -%}
    {%- set delete_sql = 'delete from ' ~ old_relation.render() -%}
    {% call statement('_') -%}
      {{ delete_sql }}
    {%- endcall %}
    {{ return(delete_sql) }}
  {%- endif -%}
{% endmacro %}
