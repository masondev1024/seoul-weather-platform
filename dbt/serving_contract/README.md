# Serving Contract v1 validation harness

도메인 공통 Serving Contract를 사람 리뷰가 아니라 **CI가 강제**한다. 규격 원천은
ASAC-DAG `docs/contracts/serving-contract-v1.md` (#478 최종 결정). 이 디렉터리는
그 문서의 **structural 규칙을 machine-readable로** 옮긴 `schema.yml` 과, 그것을
구동하는 validator·CLI·테스트다. `domains/traffic_weather/contracts/engine` 의
stable-facade·exit code(0/1/2)·결정적 UTF-8 report 관례를 재사용한다.

## 구성

| 파일 | 책임 |
| --- | --- |
| `schema.yml` | 필수/선택/제외 필드·허용값·기본값·legacy 키의 machine-readable 정의 |
| `model.py` | dbt schema YAML·manifest → `ServingModel` 추출 |
| `validator.py` | structural(스키마 구동) + semantic + global 규칙 → findings |
| `cli.py` / `validate_serving_contract.py` | stable CLI facade. exit `0`=PASS `1`=FAIL `2`=ERROR |
| `tests/` | 정상·실패 Fixture + behavioral oracle |

## 사용

```bash
# 계약 선언(meta.serving)이 있는 dbt yml 검증. manifest는 선택(컬럼 실존·멤버십 강화).
python serving_contract/validate_serving_contract.py \
  --source "domains/**/*.yml" \
  [--manifest domains/<domain>/target/manifest.json] \
  --format text
```

`meta.serving` 블록이 없는 모델은 계약 대상이 아니므로 조용히 건너뛴다(마이그레이션
전 기존 `serving_tier`/`serving_gold_candidate` 파일은 "0 models"). manifest를 주면
`primary_key` 컬럼 실존과 manifest 멤버십을 추가로 검증한다.

## 강제 규칙

- `required_field_missing` — 필수 9필드 누락
- `required_field_invalid` / `invalid_enum_value` / `invalid_field_format` — 타입·허용값·형식(`product_id` 패턴)
- `product_id_duplicate` — product_id 전역 유일성
- `external_enabled_conflict` — `external=true` 인데 `enabled≠true`
- `primary_key_not_a_column` / `primary_key_evidence_missing` — PK 컬럼 실존 + `not_null`·고유성 근거
- `excluded_field_present` — `estimated_*`·`api_*` 등 실측/Worker 소유 필드 선언
- `legacy_double_declaration` — 구 메타(`serving_tier`·`external`·`refresh`·`serving_gold_candidate`) + `meta.serving` 이중 선언
- `publication_trigger_invalid` — cron·asset 정확히 하나
- `partial_policy_invalid` / `reliability_invalid` — 정책 값 범위·키
- `conditional_required_missing` — `event_time` 또는 `freshness_field` 선언 제품의 `freshness_slo_minutes` 누락 (`schema.yml`의 `conditional_required`로 구동)
- `upsert_strategy_invalid` — `upsert_strategy`를 `publication_mode: upsert` 이외의 제품에 선언
- `public_projection_invalid` — **v1.4**: `public_projection` exact key, semver, non-empty unique physical identifier list 위반
- `public_projection_unknown_column` — projection 컬럼이 manifest 또는 YAML column 계약에 없음
- `freshness_field_not_a_column` — 명시한 실제 freshness 축이 YAML/manifest 모델 컬럼에 없음
- `public_projection_required_field_missing` — projection에 primary key, `event_time`, `freshness_field`, `reliability.sample_count_field` 누락
- `public_projection_internal_field` — raw/request/run lineage 또는 secret-like identifier를 public projection에 노출
- `public_projection_column_metadata_missing` — projected column의 `description`, `data_type`, `semantic_role`, `nullable`, `null_meaning`, `unit` 누락
- `public_projection_nullability_conflict` — **v1.4**: projected column의 dbt `not_null` 테스트와 `nullable: true` 동시 선언
- `source_evidence_invalid` — **v1.5**: 출처 식별자·공개 HTTPS URL·권리 확인일·출처표시·재배포 범위 중 하나가 불완전하거나 모호함
- `source_evidence_unknown_field` / `source_evidence_duplicate` — 출처 증거의 스펙 밖 필드(오타 가능성) 또는 모델 안의 `source_id` 중복
- `quality_coverage_invalid` / `quality_coverage_unknown_field` — **v1.6**: coverage 축·기대 distinct 수·최소 비율이 재현 불가하거나 스펙 밖 필드가 있음
- `model_not_in_manifest` — manifest에 없는 모델 선언 (manifest 제공 시)

> 참고: 문서 §8의 "알 수 없는 `contract_version` → ERROR"는 **Publisher 런타임**의 책임이다.
> 정적 validator에서 미지원 버전은 `invalid_enum_value`(FAIL, exit 1)로 잡고, exit 2는 invocation/IO 오류로만 예약한다.

## 테스트

```bash
python -m pytest -q serving_contract/tests -p no:cacheprovider
```
