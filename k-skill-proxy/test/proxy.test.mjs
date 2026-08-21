import assert from "node:assert/strict";
import test from "node:test";

import worker from "../src/index.js";


const ORIGIN = "https://ask-seoul-weather-personal-e2e.example.workers.dev";
const TOKEN = "test-service-token";

function environment({ allowed = true, originFetch = async () => Response.json({}) } = {}) {
  return {
    ASK_SEOUL_ORIGIN: ORIGIN,
    ASK_SEOUL_SERVICE_TOKEN: TOKEN,
    ASK_SEOUL_ORIGIN_SERVICE: {
      fetch: originFetch,
    },
    KSKILL_RATE_LIMITER: {
      async limit() {
        return { success: allowed };
      },
    },
  };
}

test("maps the public data route to the fixed authenticated weather-risk origin", async (t) => {
  let upstreamRequest;
  const originFetch = t.mock.fn(async (request) => {
    upstreamRequest = request;
    return Response.json({
      bundle_id: "seoul-weather-risk",
      product_id: "weather_place_risk_window",
      publication_id: "publication-1",
      row_count: 0,
      limit: 100,
      time_axis: "forecast_at",
      has_more: false,
      next_cursor: null,
      usage: { used: 1, daily_quota: 1000 },
      rows: [],
    });
  });

  const response = await worker.fetch(
    new Request("https://proxy.example/v1/ask-seoul/weather-risk/data?place_id=seoul_admd_1171065000&from=2026-08-22%2000%3A00%3A00&to=2026-08-22%2023%3A59%3A59&limit=100"),
    environment({ originFetch }),
  );

  assert.equal(response.status, 200);
  assert.equal(
    upstreamRequest.url,
    `${ORIGIN}/skill/v1/products/weather_place_risk_window/data?place_id=seoul_admd_1171065000&from=2026-08-22+00%3A00%3A00&to=2026-08-22+23%3A59%3A59&limit=100`,
  );
  assert.equal(upstreamRequest.headers.get("authorization"), `Bearer ${TOKEN}`);
  assert.equal(response.headers.get("cache-control"), "no-store");
  assert.equal(originFetch.mock.callCount(), 1);
});

test("rejects unsupported paths, methods, and query parameters before upstream fetch", async (t) => {
  const upstream = t.mock.fn(async () => {
    throw new Error("must not reach upstream");
  });

  const scenarios = [
    new Request("https://proxy.example/v1/ask-seoul/weather-risk/unknown"),
    new Request("https://proxy.example/v1/ask-seoul/weather-risk/data?sql=select+1"),
    new Request("https://proxy.example/v1/ask-seoul/weather-risk/data", { method: "POST" }),
  ];

  for (const request of scenarios) {
    const response = await worker.fetch(request, environment({ originFetch: upstream }));
    assert.ok([400, 404, 405].includes(response.status));
  }
  assert.equal(upstream.mock.callCount(), 0);
});

test("fails closed when configuration is missing and limits public traffic", async (t) => {
  const upstream = t.mock.fn(async () => {
    throw new Error("must not reach upstream");
  });

  const missingSecret = await worker.fetch(
    new Request("https://proxy.example/v1/ask-seoul/weather-risk/bundle"),
    { ...environment({ originFetch: upstream }), ASK_SEOUL_SERVICE_TOKEN: "" },
  );
  const limited = await worker.fetch(
    new Request("https://proxy.example/v1/ask-seoul/weather-risk/bundle"),
    environment({ allowed: false, originFetch: upstream }),
  );

  assert.equal(missingSecret.status, 503);
  assert.equal((await missingSecret.json()).code, "upstream_not_configured");
  assert.equal(limited.status, 429);
  assert.equal(limited.headers.get("retry-after"), "60");
  assert.equal(upstream.mock.callCount(), 0);
});
