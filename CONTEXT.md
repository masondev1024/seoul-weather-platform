# Seoul Weather Platform Context

이 문서는 저장소에서 반복해서 사용하는 도메인 용어의 의미를 고정한다.

## Source Snapshot

고정된 upstream commit의 파일 bytes 집합이다. 이관 입력의 정본이며 현재 upstream branch나 dirty working tree를 의미하지 않는다.

## Platform Product

Weather dbt graph가 생산하고 publication lane이 게시하는 데이터 제품이다. 현재 public exact set은 `weather_place_current_outlook`, `weather_place_precipitation_window`, `weather_place_risk_window`, `weather_place_forecast_change_daily` 네 개다.

## K-Skill Product

설치된 skill이 사용자에게 노출하는 제품이다. 현재 `seoul-weather-risk`는 `weather_place_risk_window` 하나만 노출하며 runtime 정본은 `NomaDamas/k-skill`이다.

## Compatibility Place Reference

K-Skill 입력 및 기존 Weather serving과 호환되는 427개 장소 reference다. 공식 행정동 축과 목적이 다르며 자동으로 덮어쓰지 않는다.

## Canonical Admin Axis

공공 원천을 통해 주기적으로 갱신하는 공식 행정동 축이다. 현재 행정구역을 설명하지만 K-Skill의 427-place release를 자동 변경하지 않는다.

## Admin Grid Bridge History

canonical 행정동과 KMA grid 간 연결의 시점별 증거다. 현재 compatibility reference와 동일한 개수나 의미를 보장하지 않는다.

## Publication Evidence

제품의 publication, freshness, coverage, query availability를 증명하는 metadata다. dbt companion의 content evidence와 publisher가 추가하는 publication evidence를 함께 가리킨다.

## Contract Fixture

API 응답과 오류 의미를 검증하기 위한 고정 자료다. 실제 Worker, origin, D1 배포 또는 가용성을 증명하지 않는다.

## Generated Artifact

검증된 source input에서 결정적으로 생성되는 장소 JSON, checksum, release manifest다. K-Skill upstream PR의 입력이며 설치되는 runtime 자체의 정본은 아니다.
