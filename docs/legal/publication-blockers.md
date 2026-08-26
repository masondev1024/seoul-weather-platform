# 공개 전환 법적·운영 확인

기준일: 2026-08-21

이 저장소에는 2026-08-21에 승인받은 팀 코드 재게시 사실을 `NOTICE`와
`provenance/source-files.jsonl`에 기록했다. 출처 저장소·커밋·경로·hash·파생 방법·검사기
정보는 그대로 두고, 공개 허가 상태와 사유만 갱신했다.

## 저장소 안에서 통과한 항목

- Apache-2.0 `LICENSE`와 출처를 적은 `NOTICE`가 있다.
- 추적하는 파일마다 공개 재게시 승인이 `provenance/source-files.jsonl`에 있다.
- CI는 GitHub가 제공하는 실행기만 사용하고 self-hosted 경로가 없다.
- 수동 배포 workflow는 꺼져 있고 실제 작업을 하지 않는다.
- 당시 감사에서 GitHub Release 0개, 내려받을 산출물 0개, Actions 기록 121개를 검사했다.

## GitHub 공개 전환 확인

2026-08-22에 읽기 전용으로 다음을 확인했다.

- 저장소 공개 상태 `PUBLIC`
- 저장소 변수 `WEATHER_DEPLOYMENT_ENABLED=disabled`,
  `WEATHER_GOVERNANCE_MODE=public`
- 등록된 runner 0개
- `main`에 `CI / required`와 PR 검토 규칙 적용
- GitHub Release와 내려받을 Release 산출물 0개

이 문서는 상태를 기록할 뿐이다. 소스 내용을 복사하지 않고, 비밀값을 공개하지 않으며,
앞으로의 공개·Release·workflow·runner·운영 변경을 승인하지 않는다.
