# 서울 날씨 데이터 플랫폼

서울 날씨를 수집하고, 변환하고, 품질을 확인한 뒤, 개인 Cloudflare 저장소에서
제공하는 공개 데이터 플랫폼이다. 예보와 실황을 같은 시간·격자 기준으로 보관하므로
“며칠 전에 본 예보가 실제와 얼마나 달랐는가?”를 다시 계산할 수 있다.

## 지금 확인해야 할 상태

- 현재 공개 코드는 **수집·변환·제공 계약과 검사 코드**를 담고 있다.
- 운영 자격증명과 실제 R2·Iceberg·D1 데이터는 개인 노트북과 개인 Cloudflare 계정에만 둔다.
- 관측용 `weather_ultra_srt_ncst_bronze` DAG는 기본적으로 멈춤 상태이며 schedule도 비어 있다.
- 코드 검증만으로는 API 호출, Docker 재시작, DAG 실행, R2/Iceberg/D1 쓰기를 하지 않는다.
- 관측 품질 분석용 Gold는 내부 분석용이다. D1·Worker·현재 제공 제품에는 자동으로 섞지 않는다.

## 이 저장소가 맡는 일

- KMA 날씨 수집과 Bronze 적재 DAG
- Weather `dbt` 변환 그래프와 품질 분석 모델
- D1 발행 호환 코드와 최신 정상 발행본 보존
- 개인 Weather origin 앞의 최소 경로 `k-skill-proxy`
- `seoul-weather-risk`가 사용할 장소 목록과 계약 검사
- 원본 파일의 출처와 변경 이력 관리

다음 영역은 이 저장소가 소유하지 않는다.

- 개인 Cloudflare 자격증명과 실제 R2·D1 데이터
- NomaDamas `k-skill` 실행 환경
- Marketplace, OAuth, 사용량 한도, MCP 화면
- Traffic·Citydata·Culture·Commerce·Transit 영역

## 현재 제공 제품

| 제품 ID | 만드는 모델 | 현재 K-Skill 공개 여부 |
|---|---|---:|
| `weather_place_current_outlook` | `gold_weather_place_current_outlook` | 아니오 |
| `weather_place_precipitation_window` | `gold_weather_place_precipitation_window` | 아니오 |
| `weather_place_risk_window` | `gold_weather_place_risk_window` | 예 |
| `weather_place_forecast_change_daily` | `gold_weather_place_forecast_change_daily` | 아니오 |

현재 `seoul-weather-risk`는 네 제품 중 `weather_place_risk_window` 하나만 보여 준다.
K-Skill 실행 환경의 정본은 이 저장소가 아니라 upstream `NomaDamas/k-skill`이다.

## 공개 코드와 개인 운영의 경계

공개 저장소에는 코드, 검사, 예시 환경 파일만 넣는다. 실제 값이 들어간
`weather-platform.prod.env`, Airflow 기록, Docker volume, 실행 로그, 서비스 키는 넣지
않는다. 개인 Worker의 서비스 토큰은 Cloudflare secret으로만 주입한다.

개인 운영을 시작할 때는 다음 순서를 지킨다.

1. 개인 R2·D1·Worker 대상과 환경 파일의 지문을 읽기 전용으로 확인한다.
2. Docker와 Airflow가 분리된 Compose 공간에서 시작되는지 확인한다.
3. DAG를 멈춤 상태로 둔 채 메모리·Trino·health를 확인한다.
4. 수집 → Bronze → Silver → Gold → D1 발행 → 감시 작업 순서로 검증한다.
5. 실행·활성화·재처리가 필요하면 별도 승인 후에만 수행한다.

## 원본 출처

가져온 코드는 현재 upstream 상태가 아니라 아래 고정 커밋에서만 복사한다.

| 출처 ID | 저장소 | 커밋 |
|---|---|---|
| `airflow_weather` | `ASAC-DE-bigkk/ASAC-DAG` | `73ff5665ffd5526c59de8be2969cf65dffaf468b` |
| `weather_dbt` | `ASAC-DE-bigkk/ASAC-DBT` | `a64292d50bd8c2a19784388828de38d2b4a8c525` |
| `weather_origin_contract` | `ASAC-DE-bigkk/ASK-Seoul-Serving` | `efe393e7a925d5798867424993daf0dbe5d55902` |
| `kskill_runtime` | `NomaDamas/k-skill` | `43edf3c0f1037a4e510b21de61e26965212b6620` |

정본 목록은 `provenance/source-refs.lock.json`, `provenance/source-inventory.json`,
`provenance/source-files.jsonl`에 있다.

## 비밀값 없이 하는 기본 검사

아래 검사는 저장소 파일과 예시 값만 사용하며 Airflow·Docker·데이터 저장소를 바꾸지 않는다.

```powershell
./tools/verify_repository.ps1
```

필요할 때는 다음처럼 영역별 검사를 따로 실행한다.

```powershell
python -m pytest tests\repository -q
python -m pytest dags\common\serving\tests -q
python -m pytest dags\domains\weather\tests -q
cd k-skill-proxy && npm test
```

Airflow 파일을 실제 실행하지 않고 가져올 수 있는지 확인하려면
`tools\verify_dagbag.ps1`를 사용한다. 이 검사는 네트워크와 자격증명이 없는 일회성
컨테이너 안에서만 `DagBag`을 읽는다.

## Airflow 변경 승인 규칙

사용자 승인 전에는 다음을 하지 않는다.

- Airflow 이미지 빌드·배포
- scheduler, dag-processor, api-server, triggerer 재시작·재생성
- DAG 활성화, 일시정지 해제, 실행 요청, backfill
- 수집·변환·발행 파이프라인 시작·중지
- R2·Iceberg·D1 쓰기

변경이 필요하면 먼저 대상 커밋, 현재 실행 중인 작업, 메모리 영향, 검사 순서,
되돌리기 방법을 보고하고 승인을 받는다. 자세한 규칙은
`docs/operations/predeployment-approval-gate.md`에 있다.

## 문서 길잡이

- `CONTEXT.md` — 저장소에서 쓰는 용어와 경계
- `README-LOCAL.md` — 개인 노트북에서 시작하는 방법과 메모리 기준
- `docs/architecture/` — 데이터 흐름과 공개·개인 영역의 경계
- `docs/operations/` — 배포 승인, 복구, 의존 리소스, 관측 파이프라인 절차
- `docs/data-engineering-decision.md` — 선택한 설계와 버린 대안
- `docs/lessonrun.md` — 실제 장애를 원인·영향·대응 순서로 복기한 기록
- `docs/superpowers/` — 구현 당시의 상세 계획과 계약 원문

## Git 주의

사용자 승인이 없으면 `stage`, `commit`, `push`, PR을 만들지 않는다. stage가 필요해도
`git add .`나 `git add -A` 대신 변경한 경로만 지정한다.
