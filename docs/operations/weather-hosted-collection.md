# Hosted 서울 단기예보 수집 복구

## 원인과 수집 소유권

2026-10-13 조사에서 R2의 마지막 `kma_vilage_fcst` 수집 날짜는 2026-09-18이었다.
로컬 Docker API는 연결되지 않았고 호스트 보호 기록은 `stopped`, `latched=true`,
`automatic_restart=false`, `docker_vm_memory_pressure`였다. 보호 정지 뒤 Airflow
수집이 재개되지 않아 최근 날짜의 raw와 완료 manifest가 없었다. 날짜 파티션은
실제 수집일이며 마지막 예보 발표 시각을 증명하는 것은 아니다.

동일 키로 최신/당일 새벽 기상청 응답을 제한 조회해 HTTP 200 및 날짜·격자 계약을
확인했다. 인증키 실패와 리포트 경로 오류가 주원인은 아니다. 실제 API 요청량과
계정 전체 할당량은 이 두 번의 조회로 증명되지 않는다.

복구 후 `Weather Raw Collection` GitHub workflow가 단기예보 raw 수집을 소유한다.
Mac의 보호 잠금, Airflow pause, Docker 중지 상태는 유지한다. 나중에 로컬 수집을
재개하려면 hosted를 먼저 비활성화하고 실행 중 작업이 끝난 것을 확인해야 한다.
서로 다른 실행기가 Airflow Pool이나 같은 API 할당량을 자동 공유한다고 가정하지 않는다.

## 수집 → 완료 근거 → 일일 품질

기존 `KmaLanding`, `KmaHttpAdapter`, `R2RawObjectStore`와 응답 계약을 재사용한다.
Airflow·Trino·Iceberg·D1·실황 데이터 가동은 이 복구 범위에 포함하지 않는다.
수집된 예보가 Silver/Gold나 공개 제품에 반영됐다고 주장하지 않는다.

- 서울 CSV의 80개 격자와 KST `02/05/08/11/14/17/20/23:00` 발표 주기를 유지한다.
- 매시간 `:47` 실행하며 발표 후 40분이 지난 최근 24시간의 주기만 조사한다.
- 한 번에 미완료 주기 최대 2개를 처리한다. 최신 미완료 주기를 먼저 처리하고,
  다음으로 가장 오래된 공백을 보수한다. 24시간보다 오래된 backfill은 수행하지 않는다.
- 주기별 고정 run ID와 R2 checkpoint가 재실행의 재개 지점이다. 실패한 부분 수집은
  정상 완료로 표시하지 않으며 검증된 raw 참조를 다음 실행에서 재사용한다.
- `load_date`는 최초 실제 수집의 KST 날짜다. 자정을 넘어 재시도해도 checkpoint의
  날짜를 유지하며 `collected_at`과 원본 예보 발표 시각을 따로 보존한다.
- 완전한 manifest가 이미 있으면 API 요청이나 완료 시각 덮어쓰기 없이 건너뛴다.
  raw는 조건부 불변 쓰기, checkpoint는 재개 상태, manifest는 완료 증거다.
  완료 검사는 raw의 내용·페이지 완전성·SHA256 metadata까지 다시 대조한다.
  손상된 과거 주기는 격리하고 최신 정상 주기는 계속 수집하되, 작업 결과는
  degraded/실패로 드러낸다. 손상된 원본이나 완료 기록을 자동 덮어쓰지 않는다.

일일 품질 workflow는 기존대로 09:17에 전날을 평가하고 21:43에 미발행 날짜만
재시도한다. 이는 수집 workflow의 프로세스 성공이 아니라 R2 완료 manifest를
읽어 실제 커버리지와 최신성을 평가한다. 장애로 미수집된 주기를 정상으로 바꾸지 않는다.
과거에 발행한 실패 리포트는 불변으로 보존한다. 복구 시점이 하루 중간이면 첫 일일
리포트의 일부 누락은 정상적인 관측 결과이며 다음 완결일의 증거로 회복을 판단한다.

## 호출·시간·쓰기 경계

1000행/page, 격자별 최대 2 pages, 원본 응답 최대 512KiB, 실행별 물리 HTTP 시도
최대 1280회, 내부 실행 예산 20분, workflow 25분이다. 재시도 대기와 HTTP 요청도
남은 예산을 소진한다. 실패 원문 URL·키·provider 응답은 출력하지 않는다.

실제 HTTP 시도 전에 R2의 당일 KST ledger를 조건부 갱신한다. ETag `If-Match` 또는
최초 `If-None-Match`로 실행 간 예산을 보존하고 하루 최대 2400회를 허용한다.
충돌·손상·권한 오류는 fail closed이며 예약 뒤 실패한 요청도 소비한 것으로 센다.
이 한도는 hosted collector 전용이다. 같은 기상청 키의 다른 소비자를 포함한 전역
할당량 또는 외부 계정의 승인 한도를 증명하지 않는다. 로컬 SQLite로 전역 한도를
증명한다고 주장하지 않는다.

완료 주기의 무결성 검증은 metadata-only 검사가 아니다. 최근 8개 주기가 모두
완료됐다면 최대 1280개 raw GET과 해시 HEAD를 수행할 수 있고, 각 객체가 상한
512KiB라면 raw 다운로드만 최대 약 640MiB/실행이다. 실제 페이지와 객체 크기를
측정한 뒤 검증 빈도 또는 검증 receipt를 개선한다. API 요청이 0이라는 사실을
R2 읽기량이나 실행 비용이 0이라는 의미로 해석하지 않는다.

R2 쓰기는 hosted 수집기의 raw run과
`ops/control/checkpoints/weather-hosted`, `ops/control/weather-hosted/api-budget`
상태에 제한한다. 삭제·버킷 생성·이력 이관은 하지 않는다. 전용 Object Read/Write
token은 기존 버킷 하나에만 부여한다. Cloudflare token의 버킷 권한은 코드의 prefix
경계보다 넓으므로 해당 token을 다른 작업에 재사용하지 않는다. 품질 리포트의
read-only token은 그대로 분리한다. 기상청 키와 R2 키는 encrypted repository
secrets에만 전달한다.

## 활성화와 되돌리기

검증된 feature → dev → main PR과 CI를 거친 뒤
`WEATHER_COLLECTION_ENABLED=enabled`일 때만 workflow가 가동된다. 수동 실행도
이 플래그와 main/repository 경계를 따른다. 최초 검증은 `only_latest=true`로
80개 격자 한 주기에 제한하고 원본 수·행·완료 manifest·재실행 멱등성을 확인한다.
그 다음 기본 실행으로 최근 공백의 제한 보수를 확인한다.

장애 시 `WEATHER_COLLECTION_ENABLED=disabled`로 새 수집을 막는다. 이미 실행 중인
workflow는 별도로 종료하거나 제한된 예산 안에서 배출한다. 삭제 없이 raw·checkpoint·
실패 기록을 보존한다. 이전 정상 raw와 기존 공개 제품은 유지된다. 코드 되돌리기도
보호된 PR 경로를 사용하며 로컬 Docker/Airflow를 자동 재시작하지 않는다.

GitHub cron은 지연되거나 누락될 수 있다. 다음 hourly 실행이 24시간 안의 공백을
재조정하지만 24시간 이상 중단과 정상 수집 완료를 보장하지 않는다. workflow 실패
알림과 일일 리포트의 fail/unmeasured를 확인하고 반복 실패는 원인을 조사한다.
