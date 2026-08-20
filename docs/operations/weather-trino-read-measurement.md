# Weather Trino 외부 read 측정 계약

## 목적

Weather 제품의 Trino 최적화 효과를 Windows 전체 네트워크 사용량과 분리해 측정한다. FS cache 적용 여부만으로 비용 절감을 선언하지 않고, 원격 object read·Trino 처리량·R2 operation·대기시간을 가능한 범위에서 각각 기록한다.

## 측정 범위

기본 측정 단위는 `product_id`, `selector`, `dag_run_id` 또는 query id, 관측 시각, 실행 환경이다. secret, raw request parameter, API key, bucket credential, private endpoint는 기록하지 않는다.

| 지표 | 의미 | 미측정 시 처리 |
|---|---|---|
| `physical_input_bytes` | Trino가 실제 물리적으로 읽은 입력 바이트 | Trino query statistics 접근 불가로 기록 |
| `processed_bytes` | Trino 연산이 처리한 논리 바이트 | 동일 |
| `cache_external_read_bytes` | FS cache miss로 외부에서 읽은 바이트 | cache metrics 접근 불가로 기록 |
| `cache_hit_bytes` 또는 hit count | 캐시 재사용량 | 동일 |
| `r2_class_b_requests` | `GetObject`/`HeadObject` 계열 요청 수 | R2 metrics 접근 불가로 기록 |
| `catalog_operation_count` | Data Catalog metadata operation 수 | Catalog metrics 접근 불가로 기록 |
| `queue_time_ms` | Resource Group 대기 시간 | query history 접근 불가로 기록 |
| `execution_time_ms` | query 실행 시간 | 동일 |
| `windows_wifi_rx_tx_bytes` | 보조적인 PC 전체 네트워크 관측값 | Settings 원본값과 측정 범위를 함께 기록 |
| `raw_page_count` / `api_request_count` | 한 Weather 수집 run의 KMA payload 수와 실제 원천 호출 수 | Airflow XCom·task log 접근 불가로 기록 |
| `raw_spool_hit_count` | Bronze가 R2 대신 검증된 로컬 payload를 읽은 횟수 | `Read KMA raw payload from local spool` log count로 기록 |
| `r2_raw_payload_get_count` | manifest가 아닌 raw payload를 R2에서 다시 받은 횟수 | R2 metrics 또는 `Downloaded KMA raw payload from R2` log count로 기록 |

Windows Wi-Fi RX/TX는 Trino/R2 비용의 대체값이 아니다. Windows Settings의 정확한 최근 24시간 값에 접근하지 못하면 임의 수치나 `docker stats` 누적값으로 대체하지 않는다.

## 해석 순서

1. public product가 bounded serving relation과 forecast window를 사용하는지 정적 guard로 확인한다.
2. 동일 selector/query를 기준으로 Trino physical input과 processed bytes를 비교한다.
3. FS cache external read/hit과 R2 Class B/Data Catalog operation을 별도로 비교한다.
4. queue·execution·memory·spill을 확인해 동시성 문제와 데이터 스캔 문제를 구분한다.
5. Windows 전체 사용량은 보조 증거로만 첨부한다.

## Raw landing → Bronze 전송 예산

개인 노트북의 정상 3시간 주기 run에서는 R2를 raw 정본으로 유지하되, 같은 run의 landing task가 검증된 payload를 기존 공유 Airflow logs volume의 `/opt/airflow/logs/_weather_raw_spool`에 원자적으로 넘긴다. 경로는 `ASK_SEOUL_WEATHER_RAW_SPOOL_DIR`로 덮어쓸 수 있다.

- R2 `PutObject`에는 `Content-MD5`를 보내 전송 중 payload 무결성을 R2가 검증하게 하고 SHA-256 custom metadata를 함께 기록한다. R2 S3 호환표는 `PutObject`의 `Content-MD5`를 지원한다: [Cloudflare R2 S3 API compatibility](https://developers.cloudflare.com/r2/api/s3/api/). botocore의 자동 CRC32 flexible-checksum header는 `when_required`로 제한하고 실제 직렬화 header를 network 없는 테스트로 고정한다.
- 이 호출이 새로 만든 immutable object만 전체 payload `GetObject` 대신 `HeadObject`의 SHA-256 metadata를 확인한다. 이미 존재하던 object는 custom metadata를 정본 증거로 신뢰하지 않고 항상 전체 body hash를 검증한다.
- Bronze는 local spool SHA-256이 일치할 때만 사용하고, 누락·손상이면 R2로 fail-safe fallback한 뒤 기존 hash gate를 다시 적용한다.
- landing checkpoint 재사용도 local spool만으로 승인하지 않는다. canonical R2 object의 존재와 전체 body hash를 먼저 확인한 뒤에만 재사용한다.
- Bronze append가 성공하고 expected row count와 일치한 뒤에만 해당 spool 파일을 삭제한다. 실패·재시도 payload는 남기되 다음 수집 시 24시간을 넘긴 잔여 파일을 정리한다.

한 page로 끝나는 raw object가 `N`개인 정상 run의 payload 전송 예산은 다음과 같다. manifest 1회 GET과 KMA 원천 다운로드·R2 원본 PUT은 유지된다.

| raw payload 단계 | 변경 전 | 변경 후 |
|---|---:|---:|
| immutable write 본문 검증 | R2 payload GET `N` | R2 HEAD `N` |
| Bronze payload 입력 | R2 payload GET `N` | local spool read `N` |
| 합계 raw payload R2 GET | `2N` | `0` |

기본 80-grid·1 page 조건이면 구조적으로 raw payload GET이 cycle당 최대 160회에서 0회로 줄고, 하루 8 cycle이면 최대 1,280회가 제거된다. 이는 요청 구조의 상한 비교이며 실제 절감 바이트·비용은 payload 크기, fallback, retry, R2/Trino metrics를 같은 24시간 범위로 측정한 뒤 확정한다.

## 멀티노드 경계

이 계약은 멀티노드 도입을 전제하지 않는다. bounded query에서 OOM이 재발하거나 Worker 메모리·queue가 지속적으로 포화될 때만 별도 capacity 검토를 시작한다. 멀티노드 검토 시 Worker별 cache 중복, node-to-node exchange, R2 요청 수와 운영 비용을 함께 측정한다.

## 완료 주장

다음 중 하나라도 없으면 “인터넷 비용이 줄었다”를 확정하지 않는다.

- 동일 범위·동일 query의 전후 Trino physical input 비교
- FS cache hit/miss 또는 external read 증거
- R2/Data Catalog operation 범위
- 실행 환경과 측정 시각
- 접근 불가 지표의 명시적 미측정 사유
