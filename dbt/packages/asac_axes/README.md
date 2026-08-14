# asac_axes — 공용 dbt 패키지 (시간축 · 공간축 · 서울 행정동)

ASAC 프로젝트 전 도메인이 공유하는 **시간축/공간축 표준**과 **서울 행정동 crosswalk/경계 seed**를 담은 dbt 패키지. (issue #48)

- **표준 축**: 원천마다 제각각인 시각·좌표 표기를 매크로 하나로 통일한다.
- **canonical 코드**: 행정동은 **행안부 10자리**를 canonical 키로, **통계청 7/5자리**를 alias 로 잇는다.
- 소비 프로젝트가 `packages.yml`(local)로 참조해 **매크로 · seed · 공용 차원 모델**을 쓴다.
- Trino(Iceberg) 어댑터 기준.

## crosswalk seed → bronze source 전환 (issue #48)

행정동 **코드·명칭의 canonical 원천은 이제 bronze source**(`axes_bronze.admin_dong_master` = ASAC-DAG #154 가 적재한 행안부 행정동 마스터 Iceberg 테이블)다.
`dim_admin_dong` 모델이 이 source 의 최신 개정(revision_date)에서 서울 행정동 grain(구/시 집계행 제외)으로 차원을 만들고,
`seoul_admin_dong_crosswalk` seed 는 **원천에 없는 좌표(중심 위경도)만 보조**로 left join 한다.
즉 seed 의 역할은 (1) 좌표 보조, (2) source 부재(테이블 미적재) 환경의 폴백으로 축소됐고, 코드/명칭의 진실원천은 bronze 다.

## `dim_beop_admin_link` — 법정동↔행정동 링크 (issue #51)

같은 bronze source(`axes_bronze.admin_dong_master`)의 최신 revision 에서 서울 **법정동↔행정동 연계행**을 그대로 노출하는 view.
법정동 주소 기반 도메인(commerce 인허가 등)이 `beop_dong_code` 로 이 링크를 타고 `admin_dong_code`(행안부 10자리 canonical)로 넘어와 행정동 공통축에 조인하는 다리다.
grain 은 **(beop_dong_code, admin_dong_code) 쌍**이며 관계는 다대다(서울 최신 revision 743쌍 — 법정동 467개 중 134개가 복수 행정동에, 행정동 426개 중 91개가 복수 법정동에 걸침)라서, 법정동 하나가 행정동 하나로 결정되지 않는 경우 소비 측에서 분배 규칙(면적/균등 등)을 정해야 한다. 집계행(코드 끝 5자리 `00000`)은 dim_admin_dong 과 동일하게 제외.

## 설치 (소비 프로젝트)

```yaml
# domains/<project>/packages.yml
packages:
  - local: ../../packages/asac_axes
```

`local`은 소비 dbt project의 `packages.yml` 위치를 기준으로 해석한다. 따라서 저장소의
`domains/<project>/` 아래 project가 root의 `packages/asac_axes/`를 참조할 때는 위처럼
두 단계 상위 경로를 사용한다.

```bash
dbt deps
dbt seed --select asac_axes     # 3종 seed 적재
```

매크로는 패키지 네임스페이스로 호출한다: `{{ asac_axes.kst_at(...) }}`, `{{ asac_axes.tm_to_wgs84(...) }}`.

---

## 시간축 매크로 (`macros/time_axes.sql`)

모든 출력은 KST `timestamp(6)`. 파싱 실패는 NULL (try + regexp_like 가드).

| 매크로 | 용도 | 입력 예 |
| --- | --- | --- |
| `kst_at(expr)` | 단일 문자열 시각, 포맷 자동판별 | `'2026-07-03 17:50:59'`, `'2026-06-02 15:00:04.1127'`, `'2026-07-01 18:40'`, `'20260706132016'` |
| `kst_at_from_parts(date_col, time_col)` | `yyyyMMdd` + `HHMM`/`HHMMSS` 2컬럼 | `('20260706','1320')`, `('20260706','132045')` |
| `utc_to_kst(ts_expr)` | UTC timestamp → +9h KST | `ingested_at`(UTC) |

- `kst_at` 판별 형식: `yyyy-MM-dd HH:mm:ss(.f+)?`, `yyyy-MM-dd HH:mm`(초 생략), 14자리 `yyyyMMddHHmmss`. 그 외는 NULL.
- `kst_at_from_parts` 는 기존 traffic `topis_timestamp`(HHMM/HHMMSS)와 weather kma(HHMM)를 포괄하는 상위 호환.
- `utc_to_kst` 입력은 timestamp 가정. 문자열이면 `kst_at` 로 먼저 파싱할 것.

```sql
select
    {{ asac_axes.kst_at(raw_ts) }}                  as event_at_kst,
    {{ asac_axes.kst_at_from_parts(ymd, hm) }}      as obs_at_kst,
    {{ asac_axes.utc_to_kst(ingested_at) }}         as ingested_at_kst
from {{ source('bronze', 'foo') }}
```

---

## 공간축 매크로 (`macros/space_axes.sql`)

좌표 출력 매크로는 동일 계약을 지킨다: **`<lon> as longitude, <lat> as latitude`** (서울범위 가드 포함, 범위 밖/파싱실패는 NULL). 호출부는 뒤에 콤마만 붙인다.
서울범위 가드: **lon 126.6~127.3 / lat 37.3~37.75**.

| 매크로 | 용도 |
| --- | --- |
| `seoul_lonlat(lon_raw, lat_raw)` | 이미 WGS84 십진도인 좌표 정규화 (culture 매크로 승격) |
| `tm_to_wgs84(x_col, y_col)` | 중부원점 TM 좌표 → WGS84 근사 역변환 |
| `tm_to_wgs84_relation(relation, x_col, y_col)` | 동일 수식의 레이어드 변형 — FROM 블록 생성 (표현식 인라인 폭발 회피, 아래 주의 참조) |
| `admin_dong_contains(wkt, lon, lat)` | 행정동 point-in-polygon 술어 (`ST_Contains`) |

```sql
select
    id,
    {{ asac_axes.tm_to_wgs84(tm_x, tm_y) }},   -- longitude, latitude 두 컬럼
    ...
from {{ source('bronze', 'commerce') }}
```

### `tm_to_wgs84` — 근사 변환 주의

중부원점 TM(EPSG:5186 계열, 중앙자오선 127°·원점위도 38°·k0 1.0·false E/N 200000/500000·GRS80)의
**역 Transverse Mercator 급수 전개**를 SQL 수식으로 구현. 상수는 `scripts/build_crosswalk.py`와 동일 GRS80 파라미터로 사전 계산.

**복잡한 모델(CTE 다단 참조·incremental merge)에서는 `tm_to_wgs84_relation`을 사용한다.**
인라인 버전은 중간항(mu, fp, W, N1, R1, D…)이 Jinja 문자열 치환으로 중첩 전개돼 컴파일
표현식이 지수적으로 커진다(traffic silver 기준 73KB). Trino 는 프로젝션 하나를 단일
메서드로 코드젠하므로 `QUERY_EXCEEDED_COMPILER_LIMIT`로 실패할 수 있다(2026-07-07
traffic 장애). relation 변형은 중간항을 서브쿼리 레이어 컬럼으로 **한 번씩만** 계산해
같은 수식을 선형 크기(~10KB)로 유지한다. 출력 = `relation.*` + `longitude`/`latitude`
+ `__tm_*` 스크래치 컬럼(최종 select 에서 명시 컬럼만 뽑으면 노출되지 않음).

> **근사 변환 — 수 m~수십 m 오차. 정밀 측지 용도가 아니라 행정동 할당 용도.**
> 서울 범위에서 위경도 0.001°(~100m) 이내를 목표로 하며, 실측 검증 결과 서울시청 역산은 실제값과 lon ~57m·lat ~15m 차이(0.001° 이내)다.

검증된 픽스처 (Trino 실행):

| 원천 | TM (x, y) | 변환 결과 (lon, lat) | 위치 |
| --- | --- | --- | --- |
| traffic | 206318.237564, 445199.529199 | 127.071459, 37.506244 | 강남/송파 한강변 |
| commerce | 202600.289971836, 453416.031698163 | 127.029438, 37.580293 | 동대문구 제기동 |
| 서울시청 역산 | 198000, 451900 | 126.977362, 37.566635 | 실제 126.9780, 37.5665 |

> **성능 한계**: 이 매크로는 급수 전개를 인라인 전개하므로 컴파일 SQL이 크다(호출 1회당 약 7KB × 2컬럼).
> 한 `SELECT` 에 여러 번 쌓으면 Trino `QUERY_EXCEEDED_COMPILER_LIMIT` 가 날 수 있다.
> 통상 silver 모델에서 좌표 1쌍 변환(2컬럼)은 문제없다. 다수가 필요하면 앞선 CTE 에서 lon/lat 를 물질화해 재사용할 것.

### 행정동 할당 표준 패턴

`tm_to_wgs84`/`seoul_lonlat` 로 얻은 좌표를 `seoul_admin_dong_boundary` seed 와 point-in-polygon 조인한다:

```sql
with pts as (
    select id, {{ asac_axes.tm_to_wgs84(tm_x, tm_y) }}
    from {{ source('bronze', 'foo') }}
)
select
    p.id,
    b.admin_dong_code,   -- 행안부 10자리 (canonical)
    b.gu_code,           -- 행안부 5자리
    b.sigungu, b.dong
from pts p
left join {{ ref('asac_axes', 'seoul_admin_dong_boundary') }} b
    on p.longitude is not null
   and {{ asac_axes.admin_dong_contains('b.boundary_wkt', 'p.longitude', 'p.latitude') }}
```

---

## 제네릭 테스트 (`macros/axis_tests.sql`)

YAML 에서는 `test_` 접두어를 뺀 이름으로 참조.

| 테스트 | 인자 | 실패 조건 |
| --- | --- | --- |
| `in_seoul_bbox` | `kind: lon`\|`lat` | 컬럼이 서울 bbox 밖 (NULL 은 허용) |
| `axis_coverage` | `min_ratio: <float>` | non-null 비율 < min_ratio (커버리지/정확도 게이트) |

```yaml
columns:
  - name: longitude
    tests:
      - in_seoul_bbox:
          arguments: {kind: lon}
  - name: admin_dong_code
    tests:
      - axis_coverage:
          arguments: {min_ratio: 0.98}
```

---

## Seed (`seeds/`)

코드 컬럼은 모두 **varchar** (선행 0 보존, 숫자 오인 방지 — `dbt_project.yml` `column_types` 로 고정).
`scripts/build_crosswalk.py`가 CLI로 주입된 도메인 seed 3종에서 기계 생성한다(재실행 결정적).
공용 package 구현은 consumer domain 경로를 직접 알지 않는다.

### `seoul_admin_dong_crosswalk.csv` (420행)

행안부 ↔ 통계청 코드 crosswalk. 소스는 CLI의 `--weather-grid` 입력(행안부10)과
`--dong-boundary` 입력(통계청7)이다.
조인 키는 (구명, 동명)이며, 서수 표기 차이(행안부 `제N동` vs 통계청 `N동`)와 구분자(`.` vs `·`)를 정규화해 맞춘다.

| 컬럼 | 설명 |
| --- | --- |
| `admin_dong_code` | 행안부 행정동 10자리 — **canonical** |
| `gu_code` | 행안부 앞 5자리 (자치구) |
| `stat_dong_code` | 통계청 7자리 — **alias** |
| `stat_gu_code` | 통계청 5자리 — alias |
| `gu`, `admin_dong` | 구명 / 행정동명 (행안부 표기) |
| `latitude`, `longitude` | 행정동 중심점 (weather 격자 매핑) |
| `snapshot_ref` | 원천 스냅샷 표기 (예: `kma_admin_dong_grid_20260325`) |

### `seoul_admin_dong_boundary.csv` (423행)

`--dong-boundary` 입력에 `admin_dong_code`(행안부10)·`gu_code`를 부가한다. 기존 통계청
컬럼(`sigungu_code`,`dong_code`)·`boundary_wkt`는 하위 호환을 위해 유지한다.
행 재편(신설동/용두동↔용신동, 상일1·2동↔상일동, 일원2동↔개포3동)으로 **3행은 `admin_dong_code` 공란**(gu_code 는 채움).

### `seoul_gu_boundary.csv` (25행)

`--gu-boundary` 입력에 `gu_code`(행안부 5자리)를 부가하고 기존 컬럼을 유지한다.

---

## 한계 / 경고

- **경계는 단순화 버전**입니다(kang 경고). ~0.8% 미매핑·경계 근처 오배정(예: DDP, 강남/송파 한강변) 가능. 정밀 경계 교체는 후속 이슈로.
- **`tm_to_wgs84` 는 근사 역변환**입니다. 행정동 할당 용도이며 정밀 측지에는 부적합.
- crosswalk 미매칭(총 10건, 소스 재편 기인): weather-grid 입력 7건(신설동·용두동·항동·개포3동·위례동·상일1·2동), dong-boundary 입력 3건(상일동·일원2동·용신동). `scripts/build_crosswalk.py` 재실행 시 stderr 리포트로 재확인 가능.

## seed 재생성

입력 파일의 소유권은 소비 domain에 있다. 공용 package는 consumer 경로를 알거나 추측하지
않으며, versioned source가 명시적으로 주입되지 않으면 builder는 즉시 실패한다.

```bash
python packages/asac_axes/scripts/build_crosswalk.py \
  --weather-grid /path/to/weather-grid.csv \
  --dong-boundary /path/to/dong-boundary.csv \
  --gu-boundary /path/to/gu-boundary.csv \
  --output-dir /path/to/generated-seeds
```

`--weather-grid`, `--dong-boundary`, `--gu-boundary`는 각각 CSV file path를 받으며,
`--output-dir`는 directory path를 받는다. `--output-dir`을 생략하면
`packages/asac_axes/seeds`에 출력하고, 매칭/미매칭 리포트는 stderr로 출력한다.
