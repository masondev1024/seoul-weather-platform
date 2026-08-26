# Weather 예보 품질 Gold 파이프라인 설계

> 사람용 안내: 단기예보 발표본과 시간별 실황을 비교하는 내부 분석 제품 설계다. 모델명,
> 상태 값, 설정 키, 명령어는 실행 코드와 맞춰야 하므로 원래 표기를 유지한다.

## 상태와 결정

개인 Seoul Weather 레이크하우스에 재현 가능한 예보 품질 제품을 추가한다. 기존 단기예보
이력과 시간별 KMA 실황 Bronze를 비교하고, 분석 결과는 R2가 붙은 Iceberg Gold에만 저장한다.

이 제품은 사용자 제공 제품이 아니다. D1 목록에 등록하지 않고, D1·Worker·현재 제공
자산으로 내보내지 않는다. 1차 버전은 매일 한 번 D-1·D-2·D-3 발표본의 기온과 강수를
비교한다.

## 목표

1. 같은 유효 시각을 가리킨 예보가 D-3에서 D-2, D-1로 바뀌며 얼마나 좋아졌는지 측정한다.
2. 격자별 예보·실황 근거를 남겨 모든 합계를 다시 계산할 수 있게 한다.
3. 시간별·일별 묶음에 표본 수, 일치율, 제외 이유, 실황 수정본, 근거 상태를 함께 기록한다.
4. 직전 KST 하루와 제한된 7일 보수 범위를 다시 계산해 늦게 온 실황을 멱등적으로 반영한다.
5. Trino가 파티션을 골라 읽고, 한 번에 하나만 처리하며, 시간·메모리·관측 기준을 지키게 한다.

## 하지 않는 것

- D1·Worker·챗봇·에이전트·공개 제공 발행
- 실시간에 가까운 KMA 값을 제공자가 확정한 최종 정답이라고 주장
- 1차 버전에서 초단기·중기예보 평가
- 습도·바람·적설·강수량 자체의 점수 계산
- 관측소 보간, ASOS 비교, 과거 실황 어댑터
- 전체 이력 자동 재생성, 범위 없는 `dbt full refresh`
- 관측이 완전하지 않은 기간을 추정해 채우는 backfill

## 검토한 방법과 선택

### A. Trino에서 SQL/`dbt` 증분 계산 — 선택

Trino가 날짜로 제한한 Iceberg 예보·실황을 읽고, `dbt`가 정규화·비교 모델을 만들며,
Airflow가 매일 한 번 실행한다. 스케줄러 프로세스가 많은 행을 직접 들고 있지 않아도 되고,
현재 레이크하우스의 manifest·검사·실행 지표와 맞는다. Python 품질 계산과 SQL 의미가
달라질 위험이 있으므로 고정 자료를 이용한 일치 검사를 필수로 둔다.

### B. Python 품질 계산 일괄 작업

`weather_quality`를 바로 쓸 수 있지만 많은 자료를 scheduler/worker로 옮기고 대량
Iceberg 쓰기와 프로세스 메모리를 따로 관리해야 한다. 결정적인 계약 자료와 SQL 일치
검사에만 보관하고 운영 엔진으로는 선택하지 않는다.

### C. 매시간 자산 연결 계산

지표는 빨리 나오지만 Trino 경쟁이 커지고 실황 수정이 끝나기 전에 점수가 만들어지며,
늦게 온 자료 보수가 복잡해진다. 신뢰성·비용 목표와 맞지 않아 선택하지 않는다.

## 구조와 데이터 흐름

```text
날짜로 나눈 Iceberg Bronze 단기예보
             +
시간별 Iceberg Bronze 실황
             |
             v
silver_weather_quality_forecast_vintage
             +
silver_kma_observation_truth
             |
             v
silver_weather_forecast_observation_match
             |
             +-------------------------------+
             v                               v
실행 버전별 Iceberg Gold 이력                |
             |                               |
             v                               |
품질 발행 manifest                          |
             |                               |
             v                               |
최신 정상 Gold 제품 세 뷰                   |
```

운영 계산은 SQL을 기준으로 한다. Python 계산기는 계약·경계·공식·고정 검사 자료의
의미 기준으로 남긴다.

## 입력 계약

### 예보

입력은 발행 가능한 `bronze_kma_vilage_fcst`의 일부이며
`silver_weather_quality_forecast_vintage`에서 정규화한다. 기존
`silver_kma_vilage_fcst`는 날짜로 나눠지지 않아 매일 읽으면 7일 조건도 R2 전체 읽기로
커진다. 2026-08-22 읽기 확인에서는 11,862,400행, 64,979,033바이트, 두 파일이었다.

Bronze는 `load_date`로 나뉜다. 요청한 7일의 D-3 하한을 만들 수 있는 load 날짜만 읽고,
`day(valid_at)`로 나눈 전용 Silver에 쓴다. 기존 제공용 Silver를 다시 만들거나 나누지 않는다.

| KMA 범주 | 품질 변수 | 값 종류 | 단위 |
|---|---|---|---|
| `TMP` | `temperature_air_2m` | 연속값 | `degC` |
| `POP` | `precipitation_occurrence` | 확률 | 0–100을 0–1로 변환 |
| `PTY` | `precipitation_occurrence_category` | 젖음/마름 범주 | `category` |

예보 행은 숫자·범주 값, 올바른 `issued_at`·`forecast_at`, 기준 서울 격자, 출처 수정본을
가져야 한다. 틀린 값은 제외 이유를 남기고 0·마름·성공 비교로 바꾸지 않는다.

### 실황

입력은 `bronze_kma_ultra_srt_ncst`다. 정확한 80개 격자와 640개 범주 manifest 문턱을
통과한 수정본만 사용한다.

| KMA 범주 | 품질 변수 | 계산 |
|---|---|---|
| `T1H` | `temperature_air_2m` | 유한한 섭씨 값 |
| `PTY` + `RN1` | `precipitation_occurrence` | `PTY != 0` 또는 `RN1 > 0`이면 젖음 |
| `PTY` + `RN1` | `precipitation_occurrence_category` | 같은 두 값으로 `wet`/`dry` |

`PTY`나 `RN1`이 없거나 틀렸다고 마름으로 처리하지 않는다. 실황 기준 단위는
`(grid_id, observed_at, variable, truth_revision)`이다.

`evaluation_as_of`까지 보이는 수정본 중 최신 것을 고른다. 제공자 최종 수정 시각이
없으므로 실시간 값은 모두 `provisional`로 남기고, 해당 묶음은 `evidence_state=degraded`와
제한 사항을 함께 싣는다. 앞으로 과거 실황 어댑터가 최종본을 제공하면 제한 범위를 다시
계산한다.

## 발표본 선택과 시간 의미

모든 저장 시각은 UTC 순간이고, KST는 평가 날짜와 일정 경계에만 사용한다. 예보·실황은
`forecast_at = observed_at`이며 격자·변수가 같을 때만 맞춘다.

각 격자·변수·유효 시간에서 아래 구간 안의 가장 늦은 `issued_at`을 고른다.

```text
D-1: [valid_at - 27시간, valid_at - 24시간]
D-2: [valid_at - 51시간, valid_at - 48시간]
D-3: [valid_at - 75시간, valid_at - 72시간]
```

구간이 비면 `missing_vintage` 기록을 만든다. 다른 발표 거리 값을 대신 쓰지 않는다.
같은 발표 시각이 여러 개면 출처 수정본과 출처 ID로 순서를 정하며, 의미가 충돌하면
실행을 중단한다.

## 모델 계약

### `silver_weather_quality_forecast_vintage`

- Iceberg 증분 merge
- `bronze_kma_vilage_fcst.load_date` 범위와 발행 manifest로 읽기 제한
- `day(valid_at)` 파티션
- 행 단위 `(grid_id, valid_at, variable, issued_at, source_revision)`
- TMP·POP·PTY만 정규화하고 발표본·출처 수정본 보존
- 제공용 `silver_kma_vilage_fcst`를 바꾸지 않음

### `silver_kma_observation_truth`

- Iceberg 증분 merge
- `day(observed_at)` 파티션
- 행 단위 `(grid_id, observed_at, variable, truth_revision)`
- 출처 ID, 수정본, 수집 시각, 평가 노출 시각, manifest 실행, payload hash를 보존

### `silver_weather_forecast_observation_match`

- KST 날짜 범위로 제한한 Iceberg 증분 merge
- `day(valid_at)` 파티션
- 행 단위 `(grid_id, valid_at, variable, vintage_label)`
- `matched`, `missing_vintage`, `missing_truth`, `invalid_forecast`, `invalid_truth`,
  `incompatible_contract` 중 하나를 남김
- 선택한 예보·실황 수정본, 값, 연속값 오차, 확률 오차, 범주별 TP/FP/TN/FN을 보존
- 수정본이 바뀌면 같은 업무 키를 갱신하며 성공 행을 중복 추가하지 않음

### 분석 결과를 한 번에 공개하기

Gold 후보 행에는 `evaluation_run_id`, `evaluation_as_of`, 평가 KST 날짜를 붙인다. 세
행 단위와 합계 검사가 모두 통과하기 전에는 제품 뷰에서 보이지 않는다. 마지막 Airflow
작업이 품질 발행 manifest에 불변 `SUCCESS` 한 건을 추가한 뒤에만 공개한다.

제품 뷰는 날짜별 최신 성공 실행만 선택한다. 실패·시간 초과가 나도 일부 날짜가 보이지
않으며, 14일 되돌리기 기간 뒤 실패·이전 후보를 정리할 수 있다. 이 manifest는 내부
Iceberg 메타데이터이고 D1 목록에는 넣지 않는다.

### `gold_weather_forecast_quality_grid_score`

- 공개된 실행 이력의 격자 진단 뷰
- 행 단위 `(grid_id, valid_at, variable, vintage_label)`
- 합계 재현에 필요한 선택 근거와 행별 점수 요소 보유
- API 원문, 자격증명, 서명 URL, 제한 없는 조회 인자 미포함

### `gold_weather_forecast_quality_hourly`

- 공개된 실행 이력의 시간별 뷰
- 행 단위 `(valid_at, variable, vintage_label)`
- 완전한 묶음의 기준 모집단은 서울 80개 격자
- 표본 수, 예상 수, 일치율, 제외 수, 근거 상태, 실황 수정본 수, 지표를 제공
- 일치율 80% 미만·표본 30개 미만·격자 검사 실패는 `insufficient_evidence`

### `gold_weather_forecast_quality_daily`

- 공개된 실행 이력의 일별 뷰
- 행 단위 `(evaluation_date_kst, variable, vintage_label)`
- 시간별 지표의 평균을 다시 평균내지 않고 격자별 합계 요소에서 직접 계산
- 기온: MAE, RMSE, 편향
- POP: Brier 점수, 10개 보정 구간, expected calibration error
- 강수 발생: TP/FP/TN/FN, 정확도, 정밀도, 재현율, F1, 양성 비율
- POP 기준 0.5와 정책 ID를 함께 저장

모든 지표 행에 지표 종류, 단위, 좋은 방향, 정책 버전, null 규칙을 넣는다. 분모가
0이면 0이 아니라 null을 남긴다.

## 증분 계산과 재처리

예약 실행은 직전 완전한 KST 하루와 그 날짜를 끝으로 하는 정확히 7일을 다시 계산한다.
현재 진행 중인 KST 날짜는 포함하지 않는다. 예보는 필요한 `load_date`, Silver와 실황은
`day(valid_at)`·`day(observed_at)` 범위를 반드시 조건에 넣는다. 조건이 없는 모델은
계약 검사에서 실패한다.

다시 실행해도 같은 원본 수정본이면 no-op이다. 선택한 수정본이 바뀌면 같은 업무 키와
합계를 갱신한다. 7일보다 오래된 날짜는 수동 backfill DAG에서 한 날짜씩만 받으며,
범위 입력이나 열린 날짜는 Trino 작업 전에 거부한다.

## 실행 방식

### 매일 실행 DAG

- ID: `weather_forecast_quality_daily`
- 의도한 local 일정: `5 3 * * *`, `Asia/Seoul`
- 공개 기본값: schedule 없음, 생성 시 일시정지
- `catchup=False`, `max_active_runs=1`, 전체 20분 제한
- 모든 `dbt` 작업은 `trino_weather_heavy` 한 자리 사용
- 품질 쿼리 15분 제한, 작업도 15분 제한. 시간 초과면 전체 실패이며 일부 결과를
  공개하지 않는다.
- 모든 검사 통과 뒤 품질 발행 manifest와 내부 Gold 준비 자산만 만든다. 기존 제공 자산은
  건드리지 않는다.

03:05 KST에는 03:00부터 시작한 시점 저장이 같은 Trino 자리를 먼저 쓴다. 품질 작업은
겹치지 않고 기다리며, 03:45 실황 주기까지 자리를 계속 잡지 않도록 쿼리 제한을 둔다.
대기 때문에 20분을 넘으면 품질 실행만 실패하고 수집·제공은 계속한다.

### 한 날짜 수동 backfill DAG

- ID: `weather_forecast_quality_backfill`
- schedule 없음, 생성 시 일시정지
- 한 개 KST 날짜와 확인 문자열 필요
- 같은 한 자리 Trino, 쿼리 제한, 모델 계약, 발행 문턱 사용
- KMA를 호출하지 않고 Raw/Bronze를 바꾸지 않음

## 실패·복구·분리

- 실황이 없거나 일부면 제외 행과 부족한 근거로 남기며 마름이나 가짜 정답으로 바꾸지 않는다.
- 계약 충돌, 기준 격자 이탈, 중복 업무 키, 지표 범위 오류, 합계 불일치는 중단한다.
- 품질 실행 실패가 예보 수집·실황 수집·기존 Gold 시점 저장·D1·Worker를 막지 않는다.
- 원본 수정본·평가 범위·정책·`evaluation_as_of`가 같으면 재실행 결과가 같다.
- 일시적인 Trino/저장소 오류만 제한된 지수 backoff로 재시도하고 계약 오류는 재시도하지 않는다.
- 자동 복구는 7일 보수이고, 더 오래된 날짜는 한 날짜 수동 backfill이다.
- Iceberg 정리는 기존 주간 유지보수에서만 하고 품질 계산 안에서 전체 정리를 하지 않는다.

## 관측 자료

매 실행에 평가 범위와 `evaluation_as_of`, 입력·일치·제외·출력 행 수, 제외 이유와 근거
상태, 선택한 실황 수정본 수, `dbt`/Trino 시간·재시도·시간 초과, 모델별 읽은 바이트와
최고 메모리(가능한 경우), 정책 버전과 Gold 자산 버전을 남긴다.

Docker 수준 Trino 최고 메모리·OOM·재시작 차이는 컨테이너 밖 시작·활성화 보고서에 남긴다.

실행 실패·시간 초과·기준 격자 변경·입력이 있는데 일치 0건·일치율 80% 미만·합계
불일치·3일 연속 부족/누락은 알림 대상이다. 임시 실황 때문에 `degraded`가 된 것은
운영 장애가 아니라 결과의 제한 사항으로 기록한다.

## 검사 전략

### 단위·계약 검사

- 예보 범주·값 변환과 잘못된 값 처리
- 실황 T1H, PTY/RN1 변환
- D-1/D-2/D-3 포함 경계와 대체 금지
- 수정본 공개 시각, 결정적 순서, 충돌 거부
- 연속값·확률·보정·범주 손계산
- 표본 수와 일치율 문턱
- UTC/KST 월말·연말 경계

### `dbt` 자료 검사

- 모든 모델의 고유·비어 있지 않은 행 단위
- 완전한 시간 묶음은 정확히 80개 격자
- 변수·발표 거리·상태·단위·실황 품질·정책 ID 허용 목록
- 지표 범위·null·0분모 규칙
- 시간·일 지표와 격자 합계 일치
- 입력·결과 파티션이 요청 범위를 벗어나지 않음
- 품질 예보 모델은 제한된 Bronze `load_date`만 읽고 제공용 비분할 Silver를 읽지 않음
- 내부 품질 모델에 D1 제공 정보가 없고 D1 선택기에 나타나지 않음

### 통합·공격적 검사

- 결정적 80개 격자 자료에서 SQL 지표와 Python 품질 계산이 같다.
- 격자 누락·중복·실황 수정·PTY/RN1 누락·D-3 누락·늦은 수집·충돌을 각각 시험한다.
- 같은 실행을 반복하면 멱등이고, 수정된 실황은 영향 키와 합계만 바꾼다.
- 느린 쿼리가 시간 제한에 걸려도 Trino 실행이나 공개 가능한 부분 파티션이 남지 않는다.
- 비밀값 없는 기본값에서 DAG 읽기 오류가 없고 두 DAG가 일시정지다.
- 예보·실황·제공·D1·Worker 기존 검사도 통과한다.

## 반영 문턱

1. 모델보다 계약과 실패 검사를 먼저 만든다.
2. Python/SQL 일치 검사와 전체 저장소 검사를 통과한다.
3. 품질 일정이 꺼진 상태로 local Compose와 DagBag을 읽는다.
4. 읽기 전용 계획에서 두 입력 파티션 범위를 확인한다.
5. 공개 자산을 만들지 않는 한 날짜 shadow 계산을 한다.
6. 행 수·지표 합계·Trino 메모리·읽기량·기존 제공/D1 변화 없음을 확인한다.
7. shadow 증거와 운영 변경 목록을 보고 별도 승인을 받는다.
8. 그 뒤에만 local 일정 overlay를 켜고 정기 DAG를 활성화한다.

## 완료 기준

- 세 Gold 제품 뷰가 정한 행 단위와 실행 버전 이력·성공 manifest를 가지며 D1 정보가 없다.
- 전용 Silver가 제한된 발행 Bronze에서 읽고 기존 제공 Silver를 다시 만들지 않는다.
- 매일 D-1·D-2·D-3 기온·강수를 비교한다.
- 모든 합계를 격자 점수에서 다시 계산할 수 있다.
- 최근 7일은 멱등적으로 보수하고 더 오래된 날짜는 하루 수동 backfill만 허용한다.
- 불완전·희소·임시·수정·충돌 상태가 모두 명시되고 검사된다.
- 두 DAG가 공개 설정에서 움직이지 않고 제공 경로와 분리된다.
- 기존 Weather 한 자리 Trino와 날짜·시간 제한 안에서 계산한다.
- 운영 활성화 전에 저장소·`dbt`·DagBag·출처·비밀값·runtime 계약 검사가 모두 통과한다.
