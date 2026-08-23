const ROUTE_ROOT = "/v1/ask-seoul/weather-risk";
const PRODUCT_ID = "weather_place_risk_window";
const MAX_RESPONSE_BYTES = 1024 * 1024;
const UPSTREAM_TIMEOUT_MS = 10_000;
const DATA_QUERY_FIELDS = new Set([
  "place_id",
  "from",
  "to",
  "limit",
  "cursor",
]);

const ROUTES = new Map([
  [`${ROUTE_ROOT}/bundle`, "/skill/v1/bundles/seoul-weather-risk"],
  [`${ROUTE_ROOT}/product`, `/skill/v1/products/${PRODUCT_ID}`],
  [`${ROUTE_ROOT}/data`, `/skill/v1/products/${PRODUCT_ID}/data`],
]);

function jsonProblem(status, title, detail, code, extensions = {}) {
  return Response.json(
    { type: "about:blank", title, detail, code, ...extensions },
    {
      status,
      headers: {
        "cache-control": "no-store",
        "content-type": "application/problem+json; charset=utf-8",
        "x-content-type-options": "nosniff",
      },
    },
  );
}

function configuredOrigin(env) {
  if (typeof env.ASK_SEOUL_ORIGIN !== "string" ||
      typeof env.ASK_SEOUL_SERVICE_TOKEN !== "string" ||
      !env.ASK_SEOUL_SERVICE_TOKEN.trim() ||
      !env.ASK_SEOUL_ORIGIN_SERVICE ||
      typeof env.ASK_SEOUL_ORIGIN_SERVICE.fetch !== "function") {
    return null;
  }
  try {
    const origin = new URL(env.ASK_SEOUL_ORIGIN);
    if (origin.protocol !== "https:" || origin.username || origin.password ||
        origin.pathname !== "/" || origin.search || origin.hash) {
      return null;
    }
    return origin.origin;
  } catch {
    return null;
  }
}

function validateQuery(pathname, searchParams) {
  if (pathname !== `${ROUTE_ROOT}/data`) {
    return [...searchParams.keys()].length === 0;
  }
  for (const field of new Set(searchParams.keys())) {
    if (!DATA_QUERY_FIELDS.has(field) || searchParams.getAll(field).length !== 1) {
      return false;
    }
  }
  return true;
}

async function limited(env) {
  if (!env.KSKILL_RATE_LIMITER || typeof env.KSKILL_RATE_LIMITER.limit !== "function") {
    return null;
  }
  return env.KSKILL_RATE_LIMITER.limit({ key: "seoul-weather-risk-public" });
}

function responseHeaders(upstream) {
  const headers = new Headers({
    "cache-control": "no-store",
    "content-type": upstream.headers.get("content-type") || "application/json; charset=utf-8",
    "x-content-type-options": "nosniff",
  });
  for (const name of ["retry-after", "x-request-id"]) {
    const value = upstream.headers.get(name);
    if (value) headers.set(name, value);
  }
  return headers;
}

async function proxy(request, env) {
  if (request.method !== "GET") {
    const response = jsonProblem(405, "method not allowed", "GET 요청만 지원합니다.", "method_not_allowed");
    response.headers.set("allow", "GET");
    return response;
  }

  const requestUrl = new URL(request.url);
  const upstreamPath = ROUTES.get(requestUrl.pathname);
  if (!upstreamPath) {
    return jsonProblem(404, "not found", "지원하지 않는 K-skill 경로입니다.", "not_found");
  }
  if (!validateQuery(requestUrl.pathname, requestUrl.searchParams)) {
    return jsonProblem(400, "invalid request", "허용되지 않거나 중복된 query parameter입니다.", "invalid_query");
  }

  const origin = configuredOrigin(env);
  if (!origin) {
    return jsonProblem(503, "upstream not configured", "개인 Weather origin 설정이 준비되지 않았습니다.", "upstream_not_configured");
  }

  let rateLimit;
  try {
    rateLimit = await limited(env);
  } catch {
    return jsonProblem(503, "rate limiter unavailable", "요청 보호 장치를 확인할 수 없습니다.", "rate_limiter_unavailable");
  }
  if (!rateLimit) {
    return jsonProblem(503, "rate limiter unavailable", "요청 보호 장치가 설정되지 않았습니다.", "rate_limiter_unavailable");
  }
  if (!rateLimit.success) {
    const response = jsonProblem(429, "rate limited", "잠시 후 다시 시도하세요.", "rate_limited");
    response.headers.set("retry-after", "60");
    return response;
  }

  const upstreamUrl = new URL(upstreamPath, origin);
  upstreamUrl.search = requestUrl.searchParams.toString();
  let upstream;
  try {
    upstream = await env.ASK_SEOUL_ORIGIN_SERVICE.fetch(new Request(upstreamUrl, {
      method: "GET",
      headers: {
        accept: "application/json",
        authorization: `Bearer ${env.ASK_SEOUL_SERVICE_TOKEN}`,
        "user-agent": "seoul-weather-k-skill-proxy/1",
      },
      redirect: "manual",
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS),
    }));
  } catch {
    return jsonProblem(502, "upstream unavailable", "개인 Weather origin에 연결할 수 없습니다.", "upstream_unavailable");
  }

  if (upstream.status >= 300 && upstream.status < 400) {
    return jsonProblem(502, "upstream redirect rejected", "Weather origin redirect를 따르지 않습니다.", "upstream_redirect_rejected");
  }
  const body = await upstream.arrayBuffer();
  if (body.byteLength > MAX_RESPONSE_BYTES) {
    return jsonProblem(502, "upstream response too large", "Weather origin 응답 한도를 초과했습니다.", "upstream_response_too_large");
  }
  const contentType = upstream.headers.get("content-type") || "";
  if (!contentType.toLowerCase().includes("json")) {
    return jsonProblem(
      502,
      "invalid upstream response",
      "Weather origin JSON 계약을 확인할 수 없습니다.",
      "malformed_upstream",
      {
        upstream_status: upstream.status,
        upstream_content_type: contentType.slice(0, 128),
      },
    );
  }

  return new Response(body, {
    status: upstream.status,
    headers: responseHeaders(upstream),
  });
}

export default {
  fetch: proxy,
};
