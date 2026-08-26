# 서울 날씨 플랫폼 용어집

이 문서는 저장소에서 반복해서 쓰는 말의 뜻과 데이터 경계를 고정한다.

## 원본 스냅샷

고정한 upstream 커밋에서 가져온 파일의 바이트 집합이다. 현재 upstream 브랜치나
작업 중인 폴더 전체를 뜻하지 않는다.

## 플랫폼 제품

Weather `dbt` 그래프가 만들고 발행 절차가 게시하는 데이터 제품이다. 현재 공개 제품은
`weather_place_current_outlook`, `weather_place_precipitation_window`,
`weather_place_risk_window`, `weather_place_forecast_change_daily` 네 개다.

## K-Skill 제품

설치된 skill이 사용자에게 보여 주는 제품이다. 현재 `seoul-weather-risk`는
`weather_place_risk_window` 하나만 보여 주며, 실행 환경의 정본은 `NomaDamas/k-skill`이다.

## 호환 장소 목록

K-Skill 입력과 기존 Weather 제공 경로가 함께 쓰는 427개 장소 목록이다. 공식 행정동
목록과 목적이 다르므로 자동으로 덮어쓰지 않는다.

## 공식 행정동 축

공공 원천에서 주기적으로 갱신하는 행정동 기준이다. 현재 행정구역을 설명하지만
K-Skill의 427개 장소 배포를 자동으로 바꾸지는 않는다.

## 행정동–격자 연결 이력

공식 행정동과 KMA 격자를 언제 어떻게 연결했는지 남기는 증거다. 호환 장소 목록과
개수나 의미가 같다고 가정하지 않는다.

## 발행 증거

제품이 실제로 발행됐는지, 최신인지, 범위가 맞는지, 조회 가능한지를 증명하는 기록이다.
`dbt`가 남기는 내용 증거와 발행기가 남기는 발행 증거를 함께 말한다.

## 계약용 고정 자료

API 응답과 오류 의미를 검사하기 위한 고정 파일이다. 실제 Worker, origin, D1 배포나
서비스 가용성을 증명하지 않는다.

## 생성 산출물

검사한 입력으로부터 항상 같은 방식으로 만드는 장소 JSON, checksum, release manifest다.
K-Skill upstream PR의 입력이며, 설치된 실행 환경 자체의 정본은 아니다.
