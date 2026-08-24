# ops 관측 기록 — 무엇이 남고 무엇이 잠겼나

## 상태

이 저장소는 Weather 전용 fork 다. 상류에서 R2 `ops/*` 를 읽던 소비자
(`common_ops_d1_load` → Cloudflare D1 `_ops_*` 표, `common_ops_logship` → 태스크 로그)는
이관되지 않았고 ops 대시보드도 폐기됐다. `dags/` 전체에서 `_ops_run_event`,
`_ops_daily_metric`, `_ops_pipeline_expectation`, `ops_expectations` 는 0건이며
`common.ops.__init__` docstring 이 가리키는 `d1_ops` · `ingest` 모듈은 이 fork 에 없다.

읽는 쪽이 없는 기록은 R2 에 영구 적재되고 태스크 시도마다 PUT 비용만 남기므로,
**소비자 없는 기록기만** 환경변수로 잠갔다(기본 off). 소비자가 있는 기록기는 그대로 둔다.

## 기록기 지도

| 기록기 | R2 존 | 이 fork 의 소비자 | 판정 |
|---|---|---|---|
| `common.ops.run_sink.record_run` | `ops/runs` | 없음. **연결된 콜백도 없다**(`record_run` 호출자 0건) | 잠금 |
| `common.ops.product_observability.record_product_event` | `ops/product-events` | 없음. bronze `on_success_callback` · gold · `serving/dag_factory` 에서 태스크마다 write | 잠금 |
| `common.ops.product_observability.record_product_health` | `ops/product-health` | 없음. **호출자도 0건** | 잠금 |
| `common.runmetrics.MetricsR2Sink` | `ops/metrics` | 없음. transform 계열 4개 DAG 가 dbt 노드마다 write | 잠금 |
| `common.ops.contract.emit_ops_event` | `ops/<category>` | 없음. **호출자 0건**. 같은 관문(`run_sink._put_r2`)을 지난다 | 잠금 |
| `common.errors.sink.R2ErrorSink` | `ops/errors` | **있다** — Discord 실패 알림(`errors/airflow.problem_failure_callback`)과 사람이 읽는 상세 문서 | 유지 |
| `weather_ingest.common.runtime` 랜딩 체크포인트 | `ops/control/checkpoints/weather` | **있다** — 파이프라인이 되읽는다 | 유지 |
| `collection_slots.receipts` · `materializer` | `ops/control/state/...` | **있다** — `due_reconciler` 가 되읽는다 | 유지 |

control 계열은 규약상 자동 삭제 금지(R-4)이고 날짜 파티션도 없다. `common.storage` 를
직접 쓰므로 잠금 관문(`_put_r2`)을 지나지 않는다 — 경계는
`dags/common/tests/test_ops_telemetry_switch.py` 가 못박는다.

## 스위치

```
ASAC_OPS_TELEMETRY_ENABLED=1        # true · yes · on 도 같음. 미설정이면 off
```

- 관문은 두 곳뿐이다: `common.ops.run_sink._put_r2` 와
  `common.runmetrics.MetricsR2Sink._put_r2_object`. 둘 다 **R2 PUT 직전**이라
  콜백 연결·레코드 조립·반환값은 하나도 바뀌지 않는다.
  `dump_dbt_run_results` 가 돌려주는 레코드 수(태스크 `rows`)도 그대로다.
- 값은 호출마다 읽으므로 스케줄러 재시작 없이 다음 태스크부터 적용된다.
- 주입 sink(`R2ErrorSink(put_object=...)` · `MetricsR2Sink(put_object=...)`)와
  `ASAC_METRICS_DIR` 로 고른 로컬 파일 sink 는 명시적 선택이므로 잠그지 않는다.
- ops 대시보드를 다시 세우면 이 변수 하나로 되살린다. 코드는 지우지 않았다 —
  `run_sink` 는 Iceberg 기반 `record_run_metadata` 를 대체하면서 콜백 자리를 일부러 남긴
  설계다.

`emit_ops_event` 로 control 계열을 새로 흘리려면 스위치를 먼저 켜야 한다.
현재 호출자가 0건이라 실질 영향은 없지만, 잠금 관문이 그 아래에 있다는 사실은
새 기록기를 붙이기 전에 확인해야 한다.

## 이미 쌓인 R2 `ops/` 오브젝트 정리 — 제안(미실행)

R2 를 읽거나 지우는 것은 L1 작업이라 별도 승인 없이는 하지 않는다. 아래는 승인 시
그대로 밟을 수 있는 순서이며, **이 변경에서는 어떤 오브젝트도 지우지 않았다.**

### 1단계 — 읽기 전용 인벤토리 (선행 필수)

카테고리·`observed_date` 파티션별 오브젝트 수와 바이트를 먼저 센다. 삭제 대상 규모와
가장 오래된/최근 파티션을 모르면 경계를 정할 근거가 없다.

```
ops/runs/            domain=<d>/observed_date=<KST>/dag_id=<dag>/...
ops/metrics/         <domain>/observed_date=<KST>/...
ops/product-events/  observed_date=<KST>/domain=<d>/layer=<l>/...
ops/product-health/  observed_date=<KST>/domain=<d>/...
```

### 2단계 — 보관 경계

| 경계 | 대상 | 규칙 | 근거 |
|---|---|---|---|
| A. 규약 보관기간 | 전 관측 계열 | `runs` · `metrics` · `product-events` · `product-health` 400일 초과분, `errors` 180일 초과분 삭제 | `common.ops.contract.RETENTION_DAYS` — 소비자 유무와 무관하게 상류 규약이 이미 삭제하라고 정한 구간이다. 판단이 필요 없다 |
| B. 고아 정리 | 잠긴 4개 존만 | weather-only cutover 일자 − 30일보다 오래된 파티션 삭제 | 소비자가 사라진 뒤 쌓인 기록은 포렌식 가치도 30일이면 충분하다. 30일은 최근 인시던트를 되짚을 창구다 |

- `ops/errors` 는 B 대상이 **아니다**. 소비자가 살아 있으므로 A 만 적용한다.
- `ops/control/**` 와 `ops/receipts/**` 는 A·B 어느 쪽도 대상이 아니다(규약 R-4:
  상태 계열 자동 삭제 금지). 최신본이 곧 파이프라인 상태다.
- cutover 일자는 배포 ledger 에서 확정한 뒤 스크립트에 고정한다. 이 저장소에는 기록이
  없으므로 실행 전에 반드시 사람이 채운다.

### 3단계 — 실행 순서

1. dry-run 으로 삭제 후보 키 목록과 합계를 출력하고, 상태 계열 접두어가 단 하나도
   섞이지 않았음을 확인한다(접두어 allowlist 검사).
2. 가장 오래된 파티션 하나만 먼저 지우고 인벤토리를 다시 센다.
3. 나머지를 파티션 단위로 진행한다. 오브젝트 단위 반복 삭제는 하지 않는다.
4. 삭제 전 합계·삭제 후 합계·삭제한 파티션 목록을 운영 기록으로 남긴다.

### 4단계 — 재발 방지

이 스위치가 기본 off 인 한 잠긴 4개 존에는 새 오브젝트가 쌓이지 않는다. 정리는 1회성이며
주기 job 은 만들지 않는다. 대시보드를 되살리는 시점에 A 경계만 주기화하면 된다.
