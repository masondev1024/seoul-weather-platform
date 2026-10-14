# 서울 단기예보 일일 데이터 품질 리포트

로컬 리포트는 수집기를 바꾸거나 새 데이터를 수집하지 않고 기존 Bronze와 실행 manifest를
읽어 날짜별 JSON·Markdown 리포트를 생성한다. 예보와 실황을 비교하는
`weather_forecast_quality_daily`와 별개로 수집 데이터의 운영 품질을 기록한다.
현재 범위는 `kma_vilage_fcst` 단기예보다. 실황·Silver/Gold 전체 이력·D1 발행
품질을 측정한 것으로 확대 해석하지 않는다.

일일 리포트와 별도로 [hosted 수집 복구](./weather-hosted-collection.md)가 기상청
단기예보를 R2에 수집한다. 로컬 Bronze 리포트와 GitHub의 raw 완료 근거 리포트는
검사 깊이와 실행 환경이 다르며 서로의 성공을 대신 증명하지 않는다.

## 검사와 분모

| 검사 | 근거 | 판정 경계 |
|---|---|---|
| 누락 | publishable SUCCESS 실행에 속한 발표/격자별 Bronze 데이터 | 기본 80개 격자 × KST 8개 발표 주기 = 계약 기대 640개. 활성화된 DAG의 실제 예약 실행 분모를 증명하지는 않는다. |
| 중복 | 동일 DAG 실행 내 place·격자·발표·category·예보 시각 grain | 초과 행과 null grain을 실패로 기록한다. 다른 실행에서 보존한 재수집 이력을 중복으로 세지 않는다. |
| 최신성 | 격자별 마지막 검증된 예보 발표 시각과 수집 시각 | 발표 후 60분이 지난 주기와 비교한다. 기본 240분 경고·360분 실패. 오래된 예보를 오늘 backfill해도 최신 예보로 표시하지 않는다. |

manifest의 `SUCCESS`와 `is_publishable=true`, 예상/실제 행·원본 수 일치를
함께 요구한다. COALESCED 실행은 제외한다. 최신 상태 시각이 같으면서 상태가
충돌하면 임의 선택하지 않고 미측정으로 남긴다. 실패·부분 실행의 Bronze도
중복 검사에는 포함되며, 커버리지는 검증된 실행만 센다.

데이터가 없거나 읽지 못했다는 사실을 0건 문제 또는 정상으로 바꾸지 않는다.
검증된 표본이 없으면 누락 수·중복 수는 null/미측정이다. 근거가 있는 일부
격자만 발견되면 계약 기대치와의 차이를 누락으로 기록한다. 실제 스케줄이
중지/변경되어 있었다면 리포트의 계약 차이와 별도로 운영 원인을 확인한다.

## 범위·비용·권한

- 기본 평가 대상은 전날 KST 완결 날짜다. 자동 실행 시점의 상태를 `evaluation_as_of`
  로 고정한다. 현재 예보의 최신성도 이 시점으로 평가하므로 누락 평가 날짜와 구분한다.
- `load_date`는 KST 수집 날짜, `collected_at`/manifest `event_at`은 UTC-naive다.
  `base_date`/`base_time`은 KST 발표 시각이다.
- 읽기는 대상 날짜부터 평가 날짜까지 최대 3개 수집 날짜 파티션으로 제한한다.
  훨씬 늦은 backfill이나 전체 이력의 중복 여부는 포함하지 않는다.
- 조회마다 최대 60초, HTTP 요청 최대 15초, HTTP 시도 1회(추가 자동 재시도 없음),
  전체 실행 최대 240초다. 조회 종료/실패 시 취소·연결 정리를 수행한다.
- 기존 호스트 보호 상태가 latched이면 Trino 조회도 하지 않고 미측정 리포트를
  남긴다. 리포트를 위해 보호 잠금을 해제하거나 데이터 처리기를 시작하지 않는다.
- 로컬 `127.0.0.1:30586`에만 접속한다. `SELECT`만 실행하며 외부 데이터 쓰기는 없다.
  서버에서도 전용 read-only principal 권한을 부여하는 것이 권장된다.
- 조회 ID·경과 시간과 서버가 제공하는 읽기량을 기록한다. 모르는 읽기량은 null이다.
- credentials 파일을 shell로 source하지 않는다. catalog/schema와 최신성 문턱만
  whitelist로 읽고, 파일 권한 600을 요구한다. 비밀값·예외 원문은 출력하지 않는다.

## 실행

저장소 루트에서:

```bash
.venv/bin/python -m tools.weather_daily_quality
```

private local 설정 파일에 있는 `TRINO_ICEBERG_CATALOG`, `ASK_SEOUL_SCHEMA`를
사용한다. 파일이 없으면 namespace를 명시해야 하며 다른 테이블로 조용히
전환하지 않는다. `--catalog`, `--schema`, `--trino-port`, `--grid-csv`,
`--expected-grids`, `--issue-times`, `--warn-minutes`, `--error-minutes`로
확인된 운영 설정을 지정할 수 있다. grid/주기 설정은 수집기의 유효 설정과
맞춰야 한다. `--env-file`로 private 설정 위치를 바꿀 수 있다.

고정 시점 재평가 예시(해당 날짜부터 최대 2일 이내의 평가 날짜만 허용):

```bash
.venv/bin/python -m tools.weather_daily_quality \
  --report-date 2026-10-12 --as-of 2026-10-13T09:17:00+09:00
```

위 명령은 당시의 Iceberg snapshot/time travel을 재현하지 않는다. 현재 남아 있는
행·manifest를 해당 시각으로 필터링한 평가다. 삭제·coalesce·수정 때문에 과거
근거가 달라질 수 있으므로 당시 리포트 revision이 운영 감사의 원본이다.

종료 코드는 정상/경고 `0`, 관측된 품질 실패 `1`, 미측정 `2`, 설정/실행 오류 `3`다.
리포트 생성 성공과 데이터 품질 통과는 다르다. 실패·미측정 리포트도 저장한다.

## 자동 실행과 저장

macOS 시스템 시각이 KST인지 확인한 뒤 다음 명령으로 **리포트 작업만** 설치한다.

```bash
.venv/bin/python -m tools.weather_daily_quality_schedule --install
```

`com.mason.seoul-weather-daily-quality` LaunchAgent는 매일 09:17 및 로그인/등록 시
리포트를 실행한다. KeepAlive/반복 재시도는 없다. 시스템이 절전 중이면 launchd의
일정은 깨어날 때 합쳐 실행될 수 있다. 전원이 꺼져 있는 동안의 모든 날짜를
소급 생성하지 않는다. 전날 한 날짜를 평가하며 오래된 공백은 별도 감사 범위다.
리포트만 생성하며 Docker 시작, Airflow 활성화·트리거, 복구, Git 작업은 수행하지 않는다.

```text
logs/weather_daily_quality/
  YYYY-MM-DD.json              # 최신 평가, 원본
  YYYY-MM-DD.md                # 사람이 읽는 표현
  revisions/YYYY-MM-DD/*.json  # 내용 hash별 보존된 평가
  scheduler.log
  scheduler-error.log
```

동일 내용은 동일 revision을 사용한다. 평가 시각이나 근거가 달라지면 새 revision을
남기며 기존 평가를 삭제하지 않는다. 실행 기록은 Git에서 이미 제외된 `logs/`에
저장하고 출력 파일 권한은 600이다. JSON을 원자적으로 교체하며 Markdown은
재생성 가능한 표현이다. 로컬 실행기는 Git publication을 수행하지 않는다.
아래 GitHub workflow가 공개 raw 근거 리포트의 일일 publication을 담당한다.
공개할 때는 별도 정제·승인·PR 경로를 거쳐야 한다.

상태 확인:

```bash
launchctl print "gui/$(id -u)/com.mason.seoul-weather-daily-quality"
```

작업을 제거할 때는 LaunchAgent만 내리고 해당 plist를 삭제한다. 리포트와 수집
데이터는 보존한다.

```bash
launchctl bootout "gui/$(id -u)/com.mason.seoul-weather-daily-quality"
rm "$HOME/Library/LaunchAgents/com.mason.seoul-weather-daily-quality.plist"
```

## 검증

```bash
.venv/bin/python -m pytest tests/tools/test_weather_daily_quality.py \
  tests/tools/test_weather_daily_quality_evidence.py \
  tests/tools/test_weather_daily_quality_schedule.py -q
plutil -lint runtime/launchd/com.mason.seoul-weather-daily-quality.plist.example
```

fixture 테스트는 계약/실패 처리/일정 설정 검증이다. 실제 데이터의 정상 측정과
미래 예약 실행 성공은 각각 별도 운영 증거가 필요하다. 수집 중단 상태를 리포트로
남겨도 수집 복구가 완료된 것은 아니다. 복구에는 기존 Airflow 배포 승인 게이트와
호스트 보호 문턱을 적용한다.

## GitHub에서 매일 공개 집계 리포트 게시

`Weather Daily Quality`는 GitHub-hosted Ubuntu에서 KST 09:17에 실행한다.
KST 21:43 실행은 해당 날짜의 리포트 게시 실패를 재시도한다. 맥이 꺼져 있어도
실행되며, 로컬 launchd 리포트는 기존처럼 별도로 생성된다.

클라우드는 맥의 Trino에 접근할 수 없으므로 R2의 완료 raw manifest와 객체 목록을
직접 읽는다. 위 Bronze 검사와 측정 계층이 다르다. 공개 리포트의 중복은 **manifest
안의 동일 객체 참조**이며 Bronze 행 중복을 증명하지 않는다. manifest는 원본 적재
완료 근거이며 예보 payload 내용의 현재 품질을 다시 검사하는 근거는 아니다.

읽기는 전날·평가일 두 `load_date` prefix로 제한한다. 객체 최대 5,000개,
manifest 최대 64개, 개별 본문 256 KiB, 본문 합계 4 MiB까지 읽는다. 한도를
넘거나 접근이 실패하면 미측정 사유를 남긴다. 두 날짜에 원본 객체가 없다는 사실을
완전히 확인하면 수집 가용성을 실패로 기록한다. 미측정도 실제 진단 기록으로
게시하며, 실패를 정상으로 바꾸거나 빈 커밋을 만들지 않는다.

리포트만 `gh-pages/reports/YYYY-MM-DD.{json,md}`에 추가한다. 같은 날짜가
이미 있으면 읽기·커밋을 생략한다. 이 브랜치에 허용된 산출물 이외의 파일이 있으면
게시를 중단한다. 기존 이력을 유지하며 force push하지 않는다. 과거 날짜를
커밋 작성 시각으로 소급하지 않는다. `main` 코드는 feature → dev → main PR
검증 경로로 반영한다. 보고서 브랜치는 운영 DAG나 배포 코드의 source가 아니다.

작성자는 `269997727+masondev1024@users.noreply.github.com`으로 고정한다.
Git 명령의 `TZ=Asia/Seoul`로 실제 작성 시각을 `+09:00`에 기록한다. report 날짜를
커밋 시각으로 소급하지 않는다. GitHub 기여 날짜는 author timestamp의 시간대를
사용하므로 UTC runner에서 생성한 새벽 리포트가 한국 기준 전날로 잡히는 차이를
방지한다. 기존 커밋을 재작성하지 않는다.
프로젝트 GitHub Pages를 `gh-pages`에서 활성화하여 기여 집계 요건을 충족하고,
`GITHUB_TOKEN` push 후에는 Pages build API를 명시 호출한다. 공개 리포트에는
집계·시각·고정 사유 코드·생성 코드 SHA만 포함한다. 버킷명·원본 키·run ID·SQL·
endpoint·자격 증명·provider 오류 메시지는 포함하지 않는다.

인증은 기존 운영 쓰기 키 대신 해당 버킷의 `Workers R2 Storage Bucket Item Read`
권한만 가진 전용 Cloudflare account API token에서 파생한 S3 인증을 사용한다.
GitHub의 네 encrypted repository secrets는 `WEATHER_QUALITY_R2_ENDPOINT`,
`WEATHER_QUALITY_R2_BUCKET_NAME`, `WEATHER_QUALITY_R2_ACCESS_KEY_ID`,
`WEATHER_QUALITY_R2_SECRET_ACCESS_KEY`다. PR에서는 이 workflow를 실행하지 않으며
`main`의 schedule / workflow_dispatch만 읽기 인증에 접근한다. 만료 없이 설정된
키도 유출·폐기 시 회수하고 새 키로 교체한다.

Actions 실행 성공은 리포트 생성·게시 성공을 뜻한다. 수집 품질이 정상이라는
뜻은 아니다. 품질 상태는 리포트와 Actions Summary에서 확인한다.

수동 재시도는 Actions의 `Weather Daily Quality` → `Run workflow`로 수행한다.
중단은 해당 workflow를 disable하고 필요하면 전용 R2 token을 revoke한다.
기존 보고서 이력과 로컬 수집 상태에는 영향을 주지 않는다. 잘못된 일일 리포트의
정정은 별도 검토된 변경으로 처리한다. 자동 실행이 과거 날짜를 재작성하지 않는다.

GitHub schedule은 지연되거나 누락될 수 있다. 두 번의 예약은 같은 서비스 장애를
완전히 방어하지 못하며 무기한 매일 실행을 보장하지 않는다. 공개 저장소의 장기간
활동 중단 시 workflow가 자동 비활성화될 수 있다. Actions 실행 실패는 Actions에서
확인하고 알림을 설정한다. 잔디 표시는 GitHub 집계 때문에 최대 24시간 걸릴 수 있다.
