# GitHub `main` bootstrap·native protection 운영 절차

## 목적과 중단 원칙

이 절차는 `dev`의 검증된 한 commit을 최초 `main`으로 만들고, 저장소 기본 브랜치와 `dev`·`main` native branch protection을 API readback으로 확인하는 1회성 bootstrap이다. 최초 CI PR을 열거나 재실행하기 전에 `WEATHER_GOVERNANCE_MODE=guarded_private`를 설정하고 exact readback해야 하며, 완료 전까지 그 값을 유지한다. `guarded_private`는 단일 소유자 private 저장소의 사고 방지용 제한 mode다. 별도 최초 전환 승인으로 deployment flag와 runner가 준비된 경우에만, 같은 저장소의 exact `dev → main` merged PR과 그 merge SHA의 CI 증거를 매번 다시 검증해 배포를 허용한다. public/internal 전환 또는 추가 writer가 생기면 guarded 배포를 중단하고 더 강한 `protected` mode를 사용한다.

각 state mutation은 직전에 현재 대상 저장소, `bootstrap_sha`, 바뀌는 설정과 rollback/중단 지점을 다시 보고하고 새 사용자 승인을 받아야 한다. 한 번 받은 승인을 뒤의 mutation에 재사용하지 않는다. 이 문서 자체와 `plan`·`verify` 성공은 push, repository 설정 변경, runner 설치 또는 Airflow 배포 승인이 아니다.

## 실행 전 고정값

- 대상 저장소: `masondev1024/seoul-weather-platform`
- bootstrap 원본: 정확한 원격 `origin/dev` commit
- bootstrap 뒤 기본 브랜치: `main`
- `dev` required check: `CI / required`
- `main` required checks: `CI / required`, `Promotion Source / required`
- 두 required job name은 모든 workflow에서 각각 한 번만 존재하며 `.github/workflows/ci.yml`만 소유
- required check별 GitHub App ID: 상수로 고정하지 않고 exact branch-head/bootstrap SHA의 성공 check-run에서 동적으로 발견
- protection 대상 endpoint:
  - `/repos/masondev1024/seoul-weather-platform/branches/dev/protection`
  - `/repos/masondev1024/seoul-weather-platform/branches/main/protection`

로컬 dirty checkout이나 symbolic branch 이름을 `bootstrap_sha` 대신 사용하지 않는다. token, credential, `.env` 값은 명령 인자·로그·plan에 넣지 않는다.

## 최초 CI 이전 remote precondition — 새 승인 필요

CI workflow는 `WEATHER_GOVERNANCE_MODE`가 missing이거나 허용값이 아니면 fail-closed한다. 따라서 CI 구현을 올리는 최초 feature PR을 열거나 실패 run을 재실행하기 전에 대상 repository와 variable write 영향을 보고하고 새 사용자 승인을 받은 뒤 다음을 실행한다.

```powershell
gh variable set WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform --body guarded_private
gh variable get WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform
```

readback이 정확히 `guarded_private`가 아니면 최초 CI를 시작하지 않는다. 이 precondition 자체는 self-hosted runner, `WEATHER_DEPLOYMENT_ENABLED=enabled`, `Deploy Main`, public-readiness 또는 Airflow 상태 변경을 허용하지 않는다. 최초 `feat/* → dev` PR과 merge 뒤 `dev` push CI는 GitHub-hosted secretless 검증만 수행한다. 첫 배포는 여전히 target·baseline·capability read-only 보고와 명시 사용자 승인을 거친 별도 cutover에서만 가능하다.

## 변경 순서 정본

아래 순서를 바꾸지 않는다. 명령을 한꺼번에 붙여 넣지 말고, 뒤의 승인 gate와 readback을 각 단계 사이에 수행한다.

```powershell
gh auth status -h github.com
gh repo view masondev1024/seoul-weather-platform --json defaultBranchRef,visibility
gh variable set WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform --body guarded_private
gh variable get WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform
# STOP: exact guarded_private readback 뒤 최초 feat/* → dev CI를 완료한다.
git fetch origin dev
$bootstrapSha = git rev-parse origin/dev
gh variable get WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform
git push origin "${bootstrapSha}:refs/heads/main"
# STOP: default가 아직 dev인 동안 initial-main-bootstrap source와 main CI 성공을 확인한다.
gh repo edit masondev1024/seoul-weather-platform --default-branch main
python -m tools.github_protection plan --repo masondev1024/seoul-weather-platform --bootstrap-sha $bootstrapSha --output "$env:TEMP\weather-protection-plan.json"
python -m tools.github_protection apply --repo masondev1024/seoul-weather-platform --plan "$env:TEMP\weather-protection-plan.json" --confirm-bootstrap-sha $bootstrapSha
python -m tools.github_protection verify --repo masondev1024/seoul-weather-platform --expected-default main --expected-dev-check "CI / required" --expected-main-check "CI / required" --expected-main-check "Promotion Source / required"
gh variable set WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform --body protected
```

## 단계별 승인과 검증

### 1. 인증·대상·bootstrap SHA 확인

`gh auth status -h github.com`의 인증 host와 계정이 대상 저장소를 관리할 계정인지 확인한다. 실패하거나 계정이 다르면 중단하며 브라우저 세션, connector 또는 직접 token 호출로 우회하지 않는다. 이어 repository가 정확히 `masondev1024/seoul-weather-platform`이고 visibility가 예상과 같은지 확인한다.

`git fetch origin dev` 뒤 다음 값을 작업 기록에 `bootstrap_sha`로 남긴다.

```powershell
$bootstrapSha = git rev-parse origin/dev
```

40자리 lowercase commit SHA가 아니면 중단한다.

최초 `main` 생성 승인 직전에 variable을 다시 읽는다. missing, `protected` 또는 그 밖의 값이면 push하지 않고 중단한다.

```powershell
gh variable get WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform
```

### 2. 최초 `main` 생성 — 새 승인 필요

사용자에게 대상 ref `refs/heads/main`, source `$bootstrapSha`, 원격 branch 생성이라는 영향을 보고하고 새 승인을 받은 뒤에만 다음 push를 실행한다.

```powershell
git push origin "${bootstrapSha}:refs/heads/main"
```

push 직후 다음 두 독립 readback이 모두 `$bootstrapSha`와 정확히 같은지 확인한다.

```powershell
git ls-remote origin refs/heads/main
gh api "/repos/masondev1024/seoul-weather-platform/branches/main" --jq .commit.sha
```

하나라도 없거나 다르면 default branch를 바꾸지 않고 중단한다. 이 최초 생성만 branch 생성 예외이며 이후 `main` 직접 push는 허용하지 않는다. 최초 push의 `Promotion Source / required`는 일반 associated `dev → main` PR 규칙을 우회하지 않는다. 별도 `initial-main-bootstrap` validator가 `guarded_private`, raw push payload의 `created == true`, `deleted == false`, `before == 40자리 zero SHA`, exact `refs/heads/main`과 `after == bootstrap_sha`, same repository를 확인하고, read-only API에서 default branch가 여전히 `dev`이며 remote `dev`·`main` head가 모두 exact bootstrap SHA일 때만 `initial-bootstrap`으로 성공한다.

default branch는 이 main-creation CI가 끝날 때까지 `dev`로 유지한다. exact `bootstrap_sha`의 `CI / required`와 `Promotion Source / required`가 `completed/success`가 아니면 Step 3으로 가지 않는다. 일반 main push는 계속 exact merged same-repository `dev → main` PR과 matching merge SHA가 없으면 실패한다.

### 3. 기본 브랜치 전환 — 새 승인 필요

앞 단계의 두 SHA readback을 다시 제시하고, GitHub UI의 기본 PR target과 default-branch workflow 기준이 `main`으로 바뀐다는 영향을 보고해 새 승인을 받는다. 승인 뒤에만 실행한다.

```powershell
gh repo edit masondev1024/seoul-weather-platform --default-branch main
```

즉시 `gh repo view ... --json defaultBranchRef,visibility`를 다시 읽어 `defaultBranchRef.name == "main"`인지 확인한다. 아니면 중단한다.

### 4. 진단 mode 재확인 — read-only

native protection이 아직 검증되지 않았으므로 precondition의 repository variable을 다시 읽는다.

```powershell
gh variable get WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform
```

값이 정확히 `guarded_private`가 아니면 protection plan을 만들지 않고 중단한다. 이 bootstrap 단계에서는 self-hosted runner 등록·설치·시작, `WEATHER_DEPLOYMENT_ENABLED=enabled`, `Deploy Main`과 Airflow 상태 변경을 허용하지 않는다. guarded mode에는 governance read secret을 설치하지 않는다. 첫 배포는 별도 cutover의 read-only target·baseline·capability 보고와 사용자 명시 승인 뒤에만 진행하며, public/internal visibility 또는 추가 writer가 확인되면 guarded mode로 진행하지 않는다.

### 5. check-run 출현과 plan 생성 — read-only

먼저 `python -m tools.workflow_policy --repo-root .`로 두 required job name이 `.github/workflows/ci.yml`에만 각각 한 번 존재하는지 확인한다. `plan`은 mutation을 수행하지 않고 authenticated `gh api` GET만 사용하며, 다음을 모두 exact 검증한 뒤 UTF-8 JSON plan을 쓴다.

- repository identity가 `masondev1024/seoul-weather-platform`
- default branch가 `main`
- `dev`와 `main` head SHA가 모두 `$bootstrapSha`
- branch별 `/repos/masondev1024/seoul-weather-platform/actions/workflows/ci.yml/runs?branch=<branch>&event=push&head_sha=$bootstrapSha&status=success&per_page=100` 응답이 pagination으로 잘리지 않았고, exact `CI` run이 하나뿐이며 path가 suffix 없는 `.github/workflows/ci.yml`, event가 `push`, `head_branch`·head SHA가 요청한 branch·`$bootstrapSha`, 상태가 `completed/success`로 각각 일치
- 선택한 run의 `/actions/runs/<run_id>/jobs?filter=latest&per_page=100` 응답이 완전하고, branch별 required job이 exact run id/name/head branch/head SHA와 `completed/success`를 만족하며 각 `check_run_url`이 대상 repository의 서로 다른 positive check-run ID를 가리킴. 절대 URL은 host/repository/id를 검증한 뒤 `gh api`가 요구하는 exact `/repos/.../check-runs/<id>` path로만 전달
- 각 exact `check_run_url` GET에서 job name/head SHA와 `completed/success`, `app.slug == "github-actions"`, positive integer `app.id`가 일치
- workflow run/jobs 응답의 `total_count`가 반환 배열 길이와 정확히 같고 최대 100개임; missing·truncated·duplicate·malformed workflow run/job/check-run, missing/null/`-1`·wrong slug·서로 충돌하는 app source는 거부
- `dev`와 `main`이 같은 bootstrap SHA여도 generic commit check-run 목록을 섞거나 duplicate를 허용하지 않고 branch-bound workflow run에서 각각 source를 발견
- plan의 repository, bootstrap SHA, `dev`·`main` protection endpoint와 발견한 `app_id`를 포함한 payload
- top-level `plan_sha256`를 제외한 canonical JSON의 SHA-256 checksum

GitHub Actions의 App ID는 문서화된 stable 상수가 아니므로 숫자를 문서·코드에 hard-code하지 않는다. check-run 성공과 app source 결합은 protection 설정의 선행조건이지 자동 배포 활성화 증거가 아니다. required check가 아직 없거나 다른 SHA에만 있으면 기다렸다가 CI 원인을 확인하며, 이름을 임의로 바꾸거나 protection을 느슨하게 만들지 않는다.

```powershell
python -m tools.github_protection plan --repo masondev1024/seoul-weather-platform --bootstrap-sha $bootstrapSha --output "$env:TEMP\weather-protection-plan.json"
```

### 6. `dev`·`main` protection 적용 — 두 endpoint를 명시한 새 승인 필요

plan의 checksum, repository, `$bootstrapSha`, 두 exact endpoint, branch별 required checks를 사용자에게 보여 주고 새 승인을 받는다. 이 한 승인은 `apply`가 수행하는 아래 두 PUT과 각 PUT 직후의 GET readback만 허용한다.

1. `PUT /repos/masondev1024/seoul-weather-platform/branches/dev/protection`
2. 즉시 같은 `dev` endpoint를 GET하고 normalized payload 비교
3. `PUT /repos/masondev1024/seoul-weather-platform/branches/main/protection`
4. 즉시 같은 `main` endpoint를 GET하고 normalized payload 비교

`apply`는 mutation 직전에 remote repository/default branch, 두 branch SHA, branch-bound exact workflow run → required job → linked check-run과 GitHub Actions `app.id`를 다시 읽는다. plan checksum, target repository, confirmed bootstrap SHA, branch 집합, endpoint 또는 plan에 결합된 context별 `app_id`가 다르면 PUT 전에 실패한다. 각 PUT 직후 GET readback도 context와 exact `app_id`를 함께 비교한다. remote mutation은 shell string이나 token 인자 없이 authenticated `gh api` argv의 위 두 PUT만 사용한다.

```powershell
python -m tools.github_protection apply --repo masondev1024/seoul-weather-platform --plan "$env:TEMP\weather-protection-plan.json" --confirm-bootstrap-sha $bootstrapSha
```

payload는 다음 불변조건을 갖는다.

- strict required checks: `dev`는 `CI / required`, `main`은 그것과 `Promotion Source / required`; 모든 check는 plan이 발견한 positive `app_id`에 결합
- administrator/owner 포함 protection 적용
- PR 필수, stale review dismissal 사용
- solo maintainer를 위해 approval count `0`, code-owner review와 last-push approval은 비활성
- user/team/app bypass 없음
- linear history와 conversation resolution 필수
- force push와 branch deletion 금지
- branch lock, creation block, fork syncing은 비활성

### 7. 독립 verify — read-only

`verify`는 repository/default branch, `dev`·`main` branch head, 각 branch의 exact head SHA에 결합된 unique CI push run과 그 run의 required jobs/linked check-runs, 두 protection endpoint를 GET한다. 각 required check가 `completed/success`, `app.slug == "github-actions"`, positive `app.id`인지 다시 발견하고 protection readback의 context별 `app_id`와 exact 비교한다. context-only readback이나 missing/null/`-1` app binding은 `protected`가 아니다. GitHub가 personal repository의 빈 bypass fields를 생략한 경우에는 absent 또는 모든 actor list가 빈 경우만 허용한다. actor가 하나라도 있거나 required check가 missing/extra이거나 어떤 보호 flag라도 다르면 실패한다.

```powershell
python -m tools.github_protection verify --repo masondev1024/seoul-weather-platform --expected-default main --expected-dev-check "CI / required" --expected-main-check "CI / required" --expected-main-check "Promotion Source / required"
```

### 8. `protected` 전환 — verify 뒤의 새 승인 필요

`apply`와 독립 `verify`가 모두 `protected`로 종료된 증거를 제시하고 repository variable write에 대한 새 승인을 받는다. 그 전에는 실행하지 않는다.

```powershell
gh variable set WEATHER_GOVERNANCE_MODE --repo masondev1024/seoul-weather-platform --body protected
```

이 write는 native protection readback 완료만 기록한다. runner 설치·활성화, protected mode용 governance read secret, `WEATHER_DEPLOYMENT_ENABLED=enabled` 또는 Airflow 상태 변경 승인은 포함하지 않는다. runner 등록과 자동 배포 활성화는 [main 자동 배포 최초 전환 절차](./main-auto-deploy-first-cutover.md)의 read-only inventory와 STOP 보고를 마친 뒤 별도 승인으로 수행한다.

## 실패 처리와 guarded fallback

아래 중 하나라도 발생하면 성공으로 보고하지 않는다.

- repo/default branch/branch SHA, branch-bound workflow run/job/check-run identity·상태·GitHub Actions app source 불일치
- plan schema, checksum, repository, bootstrap SHA, branch endpoint 불일치
- protection PUT 또는 직후 GET 실패
- private 저장소 protection에서 HTTP 403 또는 404
- normalized readback 불일치

private protection의 403/404는 `guarded_private`로 남아야 한다는 진단이다. plan이나 CI 결과로 `protected`를 추론하지 않는다. `dev` PUT 뒤 `main`에서 실패한 부분 적용 상태도 전체 실패다. 이미 생긴 보호를 삭제하거나 약화해 되돌리지 말고, `guarded_private`를 유지하거나 별도 승인으로 다시 설정한 뒤 중단한다. guarded 배포는 private 단일 소유자 경계가 유지되고 별도 최초 cutover가 명시 승인된 경우에만 고려한다.

실패 시 임시 plan은 내용을 출력하지 않고 삭제한다.

```powershell
Remove-Item -LiteralPath "$env:TEMP\weather-protection-plan.json"
```

문제를 해결한 뒤에는 새 `bootstrap_sha`·check-run·plan checksum을 다시 확인하고 mutation마다 새 승인을 받아 처음부터 재평가한다. 어떤 실패 경로에서도 self-hosted runner, protected mode용 governance read secret, deployment enable flag 또는 Airflow pipeline을 활성화하지 않는다.
