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

Windows Wi-Fi RX/TX는 Trino/R2 비용의 대체값이 아니다. Windows Settings의 정확한 최근 24시간 값에 접근하지 못하면 임의 수치나 `docker stats` 누적값으로 대체하지 않는다.

## 해석 순서

1. public product가 bounded serving relation과 forecast window를 사용하는지 정적 guard로 확인한다.
2. 동일 selector/query를 기준으로 Trino physical input과 processed bytes를 비교한다.
3. FS cache external read/hit과 R2 Class B/Data Catalog operation을 별도로 비교한다.
4. queue·execution·memory·spill을 확인해 동시성 문제와 데이터 스캔 문제를 구분한다.
5. Windows 전체 사용량은 보조 증거로만 첨부한다.

## 멀티노드 경계

이 계약은 멀티노드 도입을 전제하지 않는다. bounded query에서 OOM이 재발하거나 Worker 메모리·queue가 지속적으로 포화될 때만 별도 capacity 검토를 시작한다. 멀티노드 검토 시 Worker별 cache 중복, node-to-node exchange, R2 요청 수와 운영 비용을 함께 측정한다.

## 완료 주장

다음 중 하나라도 없으면 “인터넷 비용이 줄었다”를 확정하지 않는다.

- 동일 범위·동일 query의 전후 Trino physical input 비교
- FS cache hit/miss 또는 external read 증거
- R2/Data Catalog operation 범위
- 실행 환경과 측정 시각
- 접근 불가 지표의 명시적 미측정 사유
