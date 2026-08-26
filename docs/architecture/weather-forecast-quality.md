# 예보 품질 근거 데이터 설계

## 이 설계가 증명하는 것

현재 코드는 최신 단기예보와 바로 전 예보의 차이를 비교할 수 있다. 그것은 예보판이
어떻게 바뀌었는지 보는 분석이지, 실제 날씨와의 정확도를 재는 분석은 아니다.
이 문서는 예보를 발표 시점별로 보관하고 시간별 실황과 맞춰 정확도를 다시 계산하는
별도 계약을 정의한다. 고정 시험 자료를 서울의 실제 정확도로 오해하지 않는다.

```text
검사한 서울 80개 격자
  + ForecastVintage(발표 시각, 유효 시각, 값)
  + ObservationTruth(관측 시각, 당시 보인 수정본, 평가 기준 시각)
  -> 그 시점에 알 수 있었던 예보와 실황 선택
  -> 격자별 비교
  -> 묶음별 지표와 보정 상태
  -> AI에 전달할 근거 봉투
```

이 경로는 API 호출, Docker 조작, 스케줄 변경, Trino 실행, R2/D1 쓰기, Worker 배포를
하지 않는다.

## 기준 단위와 시간

1차 버전은 격자 단위만 다룬다.

```text
예보: product_family × grid_id(nx, ny) × variable × issued_at × valid_at
실황: truth_source × truth_revision × grid_id(nx, ny) × variable × observed_at
```

기준 격자는 `dags/domains/weather/config/seoul_kma_grids.csv`의 고유한 80개
`seoul_bbox` 행이다. ID·좌표·개수·범위·인구 버전이 조금이라도 다르면 안전하게 거부한다.
79개나 81개를 임의로 채우지 않는다. 427개 행정동/장소 목록과도 섞지 않는다.

`issued_at`은 예보를 발표한 시각, `valid_at`은 예보가 가리키는 시각이다.
`collected_at`은 수집·최신성 증거일 뿐 예보 발표 시각을 정하는 데 쓰지 않는다.

## 시점별 선택 규칙

`forecast-vintage-cutoff/v1`은 D-3, D-2, D-1을 따로 선택한다. 유효 시각에서
각 기준 시각을 뺀 뒤 세 시간 여유를 포함한 구간 안에서 가장 늦은 발표본을 고른다.

```text
D-1: [valid_at - 27시간, valid_at - 24시간]
D-2: [valid_at - 51시간, valid_at - 48시간]
D-3: [valid_at - 75시간, valid_at - 72시간]
```

구간이 비어 있으면 `missing_vintage`로 남기고 다른 날짜의 발표본을 대신 쓰지 않는다.

`observation-truth-policy/v1`은 반드시 `evaluation_as_of`를 받는다. 그 시각 이후에
도착한 수정본은 보이지 않게 하고, 보이는 수정본 중 가장 최신 것을 고른다. 같은 시각에
서로 다른 값이 있으면 실패시킨다. 실시간 관측은 제공자 최종 확인 시각이 없으므로
`provisional`로 표시하고, 통과한 묶음도 `degraded` 상태로 남긴다.

이 규칙 덕분에 미래 데이터를 미리 읽는 오류를 막고 같은 입력으로 같은 결과를 다시
만들 수 있다.

## 지표와 근거 문턱

- 기온 연속값: MAE, RMSE, 편향. MAE/RMSE는 낮을수록 좋고 편향은 0이 목표다.
- 강수 확률: Brier 점수와 고정된 신뢰도 구간. Brier 점수는 낮을수록 좋다.
- 강수 발생 여부: TP, FP, TN, FN, 정밀도, 재현율, F1, 정확도, 양성 비율.
- 범주형 강수 여부: POP 확률을 자른 결과와 독립된 `occurrence`/`none` 비교.

`pop-calibration/v1`은 0.0–0.1부터 0.9–1.0까지 열 구간을 모두 보여 준다. 빈 구간도
0건과 null 평균으로 남긴다.

`metric-evidence-gate/v1`은 묶음마다 비교 표본 30개 이상, 일치 비율 80% 이상을
요구한다. 둘 중 하나라도 모자라면 `insufficient_evidence`, 실황이 임시 수정본이면
`degraded`다. 표본이 0개인 묶음도 진단용으로만 남기고 점수 결과처럼 보여 주지 않는다.
표본 수, 분모, 일치율, 기준 버전, 실황 수정본, 평가 시각, 제한 사항, 단위와 방향을
모든 지표에 함께 싣는다.

AI 응답은 “서울 날씨 정확도”처럼 범위 없는 말을 하지 못하게 한다. 반드시 묶음과
지표를 함께 말해야 한다. POP를 0.5로 잘라 계산한 정확도가 높아도 Brier 점수와
보정 구간이 나쁘면 확률 예보 품질은 낮을 수 있다.

## 고정 시험 자료

```bash
python -m weather_quality.cli \
  --grid-csv dags/domains/weather/config/seoul_kma_grids.csv \
  --scenario contracts/weather-forecast-quality/fixtures/reference-scenario-v1.json \
  --output contracts/weather-forecast-quality/fixtures/reference-evidence-v1.json

python -m weather_quality.cli \
  --grid-csv dags/domains/weather/config/seoul_kma_grids.csv \
  --scenario contracts/weather-forecast-quality/fixtures/reference-scenario-v1.json \
  --output contracts/weather-forecast-quality/fixtures/reference-evidence-v1.json \
  --check
```

이 자료는 D-1이 D-2와 D-3보다 좋도록 만든 손검산용 정답지다. 실제 서울 성능 측정값이 아니다.

## 레이크하우스에 붙일 때의 원칙

### Bronze

- 예보와 실황 원문을 수정하지 않고 요청 기록, payload hash, 출처 수정본, 수집 시각과
  원본 경로를 함께 보관한다.
- API 종류마다 별도 계약을 둔다. 초단기·중기 필드를 `kma_vilage_fcst`에 null로
  계속 덧붙이지 않는다.
- 이용 조건, 재배포 권리, 격자 연결과 품질 표시를 승인한 뒤 수집을 연다.

### Silver

- 두 개 발표본만 남기지 말고 모든 예보 발표본을 보관한다.
- 실황 수정본도 따로 정규화하고 `truth_as_of`, 품질, 수집 시각을 유지한다.
- 예보는 `day(valid_at)`, 실황은 `day(observed_at)`로 나눠 저장하고, 조인 전에 날짜
  범위를 제한한다. 그래야 Trino가 필요한 파일만 읽는다.
- 기준 식별자를 그대로 합치며, 같은 수정본의 충돌은 마지막 값으로 덮지 않고 거부한다.

### Gold

- 바뀐 유효 시각 파티션과 늦게 온 실황 범위만 다시 계산한다.
- 제품·변수·발표 정책·공간 버전·평가 범위별로 묶는다.
- 근거 문턱을 통과한 지표만 신뢰 가능한 결과로 발행하고, 부족한 묶음은 진단용으로 둔다.

### 제공과 에이전트

- 원시 숫자가 아니라 버전이 붙은 근거 봉투를 제공한다.
- 페이지·캐시 식별자에 근거 버전, 정규화한 조회, 묶음 ID를 포함한다.
- AI 응답에 지표, 제한 사항, 표본 수, 일치율, 실황 상태, 기준 시각을 함께 넣는다.
- 새 제품을 공개하려면 현재 Worker/D1 경로와 별도의 배포 검토를 거친다.

## 예보 종류별 경계

- `short_range`: 고정 시험 자료와 기존 전체 이력 Silver를 활용할 수 있다.
- `ultra_short`: 계약만 준비한 상태다. 주기·범주·실황 연결은 별도 원천 어댑터가 필요하다.
- `mid_term`: 80개 격자라고 가정하지 않는다. 지역 단위 공간 어댑터와 별도 묶음이 필요하다.

지역·장소 단위 자료를 80개 격자 묶음으로 몰래 바꾸는 어댑터는 허용하지 않는다.

## 실패·복구·관측

- 중복 식별자: 묶음을 거부하고 어떤 종류가 겹쳤는지 기록한다.
- 발표본 없음: 해당 발표 거리만 빈칸으로 남긴다.
- 미래 실황: 현재 평가 시각에서 제외하고 건수를 남긴다.
- 늦게 온 최종 실황: 영향을 받은 날짜 파티션을 다시 계산하고 새 근거 버전을 만든다.
- 오래되거나 거부된 실황: 점수 분자에서 빼고 이유를 분모 진단에 공개한다.
- 격자 일부만 존재: 진단으로 남기되 일치율 80% 문턱에서 탈락시킨다.
- 스키마 변경: 원천 어댑터와 근거 계약 버전을 올리고 호환되지 않으면 중단한다.

운영 중에는 선택·누락 발표본 수, 실황 수정본의 포함·제외 이유, 일치율, 근거 상태,
거리별 지표 변화, 확률 구간 표본, 늦은 복구량, 읽은 파일 크기, Trino 최고 메모리,
발행 나이를 기록한다. 파일 캐시는 반복 읽기를 줄일 뿐 파티션 조건과 증분 계산을
대신하지 않는다.
