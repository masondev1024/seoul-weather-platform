# Personal Weather K-skill proxy

This Worker exposes only the three read-only routes required by
`seoul-weather-risk` and forwards them to the personal Weather origin through a
same-account Cloudflare Service Binding with a scoped service token. The token
is a Cloudflare Worker secret and must never be committed or placed in
`wrangler.toml`.

Public routes:

- `/v1/ask-seoul/weather-risk/bundle`
- `/v1/ask-seoul/weather-risk/product`
- `/v1/ask-seoul/weather-risk/data`

The data route accepts only `place_id`, `from`, `to`, `limit`, and `cursor`.
Every response is `no-store`; redirects and non-JSON upstream responses fail
closed. A Cloudflare rate-limit binding protects the shared upstream quota.

Deployment requires the personal Cloudflare account in the environment and an
existing scoped origin token:

```bash
npx wrangler secret put ASK_SEOUL_SERVICE_TOKEN
npx wrangler deploy
```

Point the K-skill helper at the deployed HTTPS origin:

```bash
export KSKILL_PROXY_BASE_URL=https://seoul-weather-risk-proxy-masondev1024.<personal-subdomain>.workers.dev
```
