# GitHub `main` 최초 설정 절차

## 이 절차의 목적

검사된 `origin/dev` 한 커밋을 처음 `main`으로 만들고, 기본 브랜치와 `dev`·`main`의
보호 규칙을 실제 GitHub 응답으로 확인하는 일회성 절차다. 이 문서나 `plan`·`verify`
성공은 push, runner 설치, 자동 배포, Airflow 변경 허가가 아니다.

각 상태 변경 직전에 대상 저장소, 기준 커밋, 바뀌는 설정, 되돌리기 지점을 다시 보고하고
새 승인을 받는다. 앞에서 받은 승인을 뒤의 변경에 재사용하지 않는다.

## 고정값

- 저장소: `masondev1024/seoul-weather-platform`
- 최초 원본: 정확한 원격 `origin/dev` 커밋
- 최초 기본 브랜치: `dev`, 완료 후 `main`
- `dev` 필수 검사: `CI / required`
- `main` 필수 검사: `CI / required`, `Promotion Source / required`
- 보호 API: `/repos/masondev1024/seoul-weather-platform/branches/dev/protection`,
  `/repos/masondev1024/seoul-weather-platform/branches/main/protection`

로컬 변경 폴더나 브랜치 이름을 기준 커밋 대신 쓰지 않는다. token, 자격증명, `.env` 값은
명령 인자·로그·계획 파일에 넣지 않는다.

## 최초 CI 전에 할 일

CI는 `WEATHER_GOVERNANCE_MODE`가 없거나 허용되지 않으면 안전하게 실패한다. 최초
feature PR을 열거나 실패한 run을 다시 시작하기 전에 대상과 변수 변경을 보고하고
승인을 받은 뒤 다음을 실행한다.

```powershell
gh variable set WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform --body guarded_private
gh variable get WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform
```

읽기 결과가 정확히 `guarded_private`가 아니면 CI를 시작하지 않는다. 이 단계는 runner,
`WEATHER_DEPLOYMENT_ENABLED`, `Deploy Main`, public-readiness, Airflow 상태 변경을
허용하지 않는다.

## 변경 순서

명령을 한꺼번에 붙여 넣지 말고 각 중단 지점에서 결과를 확인한다.

```powershell
gh auth status -h github.com
gh repo view masondev1024/seoul-weather-platform --json defaultBranchRef,visibility
gh variable set WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform --body guarded_private
gh variable get WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform
# STOP: guarded_private 확인 뒤 최초 feat/* → dev CI를 끝낸다.
git fetch origin dev
$bootstrapSha = git rev-parse origin/dev
gh variable get WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform
git push origin "${bootstrapSha}:refs/heads/main"
# STOP: default가 dev인 동안 최초 main CI 성공을 확인한다.
gh repo edit masondev1024/seoul-weather-platform --default-branch main
python -m tools.github_protection plan --repo masondev1024/seoul-weather-platform --bootstrap-sha $bootstrapSha --output "$env:TEMP\weather-protection-plan.json"
python -m tools.github_protection apply --repo masondev1024/seoul-weather-platform --plan "$env:TEMP\weather-protection-plan.json" --confirm-bootstrap-sha $bootstrapSha
python -m tools.github_protection verify --repo masondev1024/seoul-weather-platform --expected-default main --expected-dev-check "CI / required" --expected-main-check "CI / required" --expected-main-check "Promotion Source / required"
gh variable set WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform --body protected
```

## 단계별 확인

### 1. 로그인·대상·커밋

`gh auth status`의 계정이 대상 저장소를 관리하는지, 저장소 이름·공개 상태가 예상과
같은지 확인한다. `git fetch origin dev` 뒤 40자리 소문자 커밋을 `bootstrap_sha`로
기록한다. 최초 push 직전 `WEATHER_GOVERNANCE_MODE`를 다시 읽는다.

### 2. 최초 `main` 만들기

새 `refs/heads/main`과 기준 커밋을 사용자에게 보고하고 승인받은 뒤에만 push한다.

```powershell
git push origin "${bootstrapSha}:refs/heads/main"
git ls-remote origin refs/heads/main
gh api "/repos/masondev1024/seoul-weather-platform/branches/main" --jq .commit.sha
```

두 읽기 결과가 모두 기준 커밋과 같아야 한다. 하나라도 다르면 기본 브랜치를 바꾸지
않는다. 이 최초 생성만 branch 생성 예외이며 이후 main 직접 push는 허용하지 않는다.

### 3. 기본 브랜치 바꾸기

최초 main의 `CI / required`와 `Promotion Source / required`가 정확한 기준 커밋에서
성공한 뒤, 영향을 보고 새 승인을 받아 실행한다.

```powershell
gh repo edit masondev1024/seoul-weather-platform --default-branch main
gh repo view masondev1024/seoul-weather-platform --json defaultBranchRef,visibility
```

결과의 기본 브랜치가 `main`이 아니면 중단한다.

### 4. 보호 규칙 계획 만들기

`python -m tools.workflow_policy --repo-root .`로 필수 검사 이름이
`.github/workflows/ci.yml`에 각각 한 번만 있는지 확인한다. `plan`은 GitHub GET만
사용해 다음을 모두 확인한다.

- 저장소와 기본 브랜치가 예상과 같다.
- `dev`·`main` 머리가 기준 커밋과 같다.
- 두 branch의 `CI` push run이 정확히 하나이고 path, event, branch, 커밋, 성공 상태가 맞다.
- 각 run의 필수 job과 연결된 check-run이 하나씩이며 성공 상태다.
- check-run의 출처가 `github-actions`이고 양의 정수 App ID가 있다.
- 페이지가 잘리지 않았고 누락·중복·잘못된 응답이 없다.
- 계획 파일에 저장소, 기준 커밋, 두 보호 endpoint, branch별 App ID, 전체 내용 checksum이 있다.

App ID는 고정 상수가 아니므로 성공한 check-run에서 매번 읽는다. 계획 파일을 만들었다고
배포가 허용되는 것은 아니다.

```powershell
python -m tools.github_protection plan --repo masondev1024/seoul-weather-platform --bootstrap-sha $bootstrapSha --output "$env:TEMP\weather-protection-plan.json"
```

### 5. 보호 규칙 적용

계획 파일의 checksum·대상·기준 커밋·두 endpoint·검사 목록을 보고 새 승인을 받는다.
`apply`는 각 PUT 직전에 위 값을 다시 읽고, PUT 직후 같은 endpoint를 GET해 확인한다.
두 branch 중 하나라도 실패하면 전체를 실패로 본다.

보호 규칙은 다음과 같다.

- `dev`: `CI / required`
- `main`: `CI / required`, `Promotion Source / required`
- 모든 검사를 계획에서 찾은 GitHub Actions App ID와 연결
- PR 필수, stale review 정리, 대화 해결 필수
- 혼자 관리하는 저장소이므로 승인 수 0, code-owner·마지막 push 승인 요구 없음
- 우회 사용자·팀·앱 없음, 강제 push·branch 삭제 금지, 선형 이력 사용

```powershell
python -m tools.github_protection apply --repo masondev1024/seoul-weather-platform --plan "$env:TEMP\weather-protection-plan.json" --confirm-bootstrap-sha $bootstrapSha
```

### 6. 독립 검증과 보호 모드 전환

`verify`는 저장소·기본 브랜치·두 branch 머리·정확한 CI run/job/check-run·두 보호
endpoint를 다시 GET한다. 검사 상태, GitHub Actions 출처, App ID, 우회 목록이 모두
맞아야 한다.

```powershell
python -m tools.github_protection verify --repo masondev1024/seoul-weather-platform --expected-default main --expected-dev-check "CI / required" --expected-main-check "CI / required" --expected-main-check "Promotion Source / required"
```

검증 결과를 제시하고 새 승인을 받은 뒤에만 `protected`로 바꾼다.

```powershell
gh variable set WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform --body protected
```

이 변경은 보호 규칙을 확인했다는 표시일 뿐 runner나 자동 배포를 켜지 않는다.

## 실패하면 어떻게 하는가

저장소·branch·커밋·run·job·check-run·App ID·계획 checksum·보호 응답이 하나라도 다르면
성공으로 보고하지 않는다. `dev`만 바뀌고 `main`이 실패한 부분 적용도 전체 실패다.
보호를 지우거나 약하게 만들어 되돌리지 말고 `guarded_private`를 유지한 채 원인을
고친다. 실패한 임시 계획 파일은 내용을 출력하지 않고 삭제한다.

어떤 실패 경로에서도 self-hosted runner, governance secret, 배포 플래그, Airflow
pipeline을 켜지 않는다.
