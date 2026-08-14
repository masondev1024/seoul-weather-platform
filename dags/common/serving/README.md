# 공통 D1 Publisher (`common/serving`)

Serving Contract v1(ASAC-DAG `docs/contracts/serving-contract-v1.md`, #478)의 **Publication을
도메인 공통 모듈로 강제**한다. #477 장애(골드가 D1에 적재됐으나 `_catalog` 미등록으로 404,
적재 잡은 exit 0)의 근본 재발 방지.

citydata `citydata_serving_export.py` 를 **복사하지 않고**, 공통화 가능한 동작과 도메인 정책을
분리했다.

| | 공통(이 모듈) | 도메인(계약에서) |
| --- | --- | --- |
| 무엇 | 게이트·write·검증·`_catalog`·smoke·메타 기록·last-known-good | 테이블 목록·mode·zero/partial/reliability·PK·event_time·trigger |

## Publication 파이프라인 (제품 1건 = 1 단위)

```
Contract Load → Publication Gate → D1 Write → row-count Verify
  → _catalog Upsert(자기 도메인) → API Smoke Test
```
하나라도 실패하면 게시 미완료(task 실패) + snapshot 은 직전 정상본 유지. 마지막에
"게시 테이블 수 == `_catalog` 등록 수" 자기검증(#477 ③).

## 파일

| 파일 | 책임 | 런타임 의존 |
| --- | --- | --- |
| `contract.py` | dbt manifest → `ServingContract` (정적 `publication_trigger` 포함) | 순수 |
| `gate.py` | zero/partial/reliability 정책 결정 | 순수 |
| `d1_client.py` | D1 접근 seam + `HttpD1Client` + `_catalog` 스키마 | 순수(+lazy requests) |
| `publisher.py` | 6단계 오케스트레이션 + 동적 메타 기록 | 순수(seam) |
| `runtime.py` | Trino reader · D1/smoke 빌더(env) | lazy trino/requests |
| `dag_factory.py` | 얇은 도메인 DAG factory | airflow |
| `watchdog.py` | D1 런타임 증거를 계약과 대조하는 읽기 전용 감시 | 순수(+lazy croniter) |

`contract`/`gate`/`d1_client`/`publisher` 는 순수라 in-memory fake로 전 경로를 단위 테스트한다
(Trino·Cloudflare·Airflow·prod 불필요).

## 동적 기록 (`_catalog` / publication)

`publication_id`·`source_run_id`·`source_row_count`·`published_row_count`·`published_bytes`·
`freshness`·`published_at`·`serving_status`(`published`/`degraded`/`skipped_retained`/`failed`).

## 독립 Freshness Watchdog

Publisher와 별도 DAG가 `_catalog.exported_at`(게시 시각)·`_catalog.freshness`·
`d1_product_quality`를 읽어 정적 `publication_trigger`·`freshness_slo_minutes`와 대조한다.
따라서 Publisher가 멈춰도 D1 미갱신을 감지하며, 방금 게시됐더라도 원천 관측시각이 낡으면
별도로 실패한다. 이 감시는 D1을 수정·재게시·자동 재시도하지 않는다.

timezone 없는 원천 관측 시각은 도메인 계약 근거가 있을 때만 제품별 timezone을 명시해 해석한다.
근거·형식·timezone이 없으면 fail-closed 한다. Transit fast tier가 첫 적용이며, 다른 도메인은
자신의 시간축·게시 주기 검증을 마친 뒤 opt-in 한다.

## 도메인 DAG (얇음)

```python
from common.serving.dag_factory import build_serving_export_dag

dag = build_serving_export_dag(
    domain="weather",
    product_ids=["weather_place_current_outlook"],
    schedule="10 * * * *",   # 계약 publication_trigger.schedule_cron 과 일치
)
```

## Secret

`CLOUDFLARE_API_TOKEN` 은 env 에서만 읽고 로그·코드에 남기지 않는다. 계정/DB id 는 비밀이 아닌
식별자로 `SERVING_CLOUDFLARE_ACCOUNT_ID`·`SERVING_D1_DATABASE_ID` env 로 주입. 공개 API base 는
`SERVING_API_BASE_URL`(없으면 smoke no-op pass — mock/local). 설정된 공개 API smoke는
`SERVING_API_SMOKE_TOKEN`을 `Authorization: Bearer`로 보내며, base URL 뒤의 `/api/v1/data/<model>`을
확인한다. 토큰 값은 env에만 두고 로그·코드·문서에 기록하지 않는다.

## 테스트

```bash
python -m pytest -q common/serving/tests -p no:cacheprovider
```
