# 개인 Weather K-Skill 프록시

이 Worker는 `seoul-weather-risk`가 읽는 세 개의 읽기 전용 경로만 공개한다. 요청은 같은
Cloudflare 계정의 개인 Weather origin으로 전달하고, origin 서비스 토큰은 Worker secret으로
보관한다. 토큰을 `wrangler.toml`이나 Git에 넣지 않는다.

## 공개 경로

- `/v1/ask-seoul/weather-risk/bundle`
- `/v1/ask-seoul/weather-risk/product`
- `/v1/ask-seoul/weather-risk/data`

data 경로에서 받는 값은 `place_id`, `from`, `to`, `limit`, `cursor`뿐이다. 모든 응답은
저장하지 않으며(`no-store`), 다른 주소로 보내는 응답이나 JSON이 아닌 upstream 응답은
안전하게 실패시킨다. Cloudflare rate-limit binding으로 공용 upstream 사용량도 제한한다.

## 배포

개인 Cloudflare 계정과 범위가 제한된 origin 토큰이 준비된 환경에서만 실행한다.

```bash
npx wrangler secret put ASK_SEOUL_SERVICE_TOKEN
npx wrangler deploy
```

배포한 HTTPS 주소를 K-Skill helper의 환경 변수로 넣는다.

```bash
export KSKILL_PROXY_BASE_URL=https://seoul-weather-risk-proxy-masondev1024.<personal-subdomain>.workers.dev
```
