// Problem 2 load test against the cluster entry point, driven by tests/load/p2-k8s.sh and tests/load/p2-ratio.sh.
//
// Shape: ramp up to VUS, hold VUS for HOLD (the spec wants >= 10000 VUs for >= 120 s), ramp down.
// Settings come from env: BASE_URL, EMAIL, PASSWORD, VUS (10000), HOLD (150s), RAMP (30s), THINK_S (1), SUMMARY_FILE.
//
// The mix is only /hazards, which goes through the Aggregator. /health never reaches it and is not rate limited, so it is left out.
// Auth: setup logs in once and sends the jwt cookie. The API refreshes only with the rotating refresh_token, which 10k VUs cannot share,
// and login is limited per IP (AUTH_RATE_LIMIT_RPS), so per VU login does not work from one machine. p2-k8s.sh and p2-ratio.sh raise
// ACCESS_TOKEN_TTL_S for the run so one token outlives it. A 401 counts as an error, so an expired token fails the run.
//
// Accounting: 200 is a success, 429 is the controlled rejection (counted apart, never an error), anything else is an error.
// Throughput is the number of 200 answers inside the hold window divided by the hold length.
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const BASE = __ENV.BASE_URL;
const VUS = Number(__ENV.VUS || 10000);
const HOLD = __ENV.HOLD || '150s';
const RAMP = __ENV.RAMP || '30s';
const THINK_S = Number(__ENV.THINK_S || 1);
const MAX_ERROR_RATE = Number(__ENV.MAX_ERROR_RATE || 0.01);

function seconds(d) {
  const m = /^(\d+)(s|m)$/.exec(d);
  if (!m) throw new Error(`duration must look like 150s or 3m, got ${d}`);
  return Number(m[1]) * (m[2] === 'm' ? 60 : 1);
}
const holdS = seconds(HOLD);
const rampS = seconds(RAMP);

// 429 is expected under the rate limit, so it is not a failed request for k6 either.
http.setResponseCallback(http.expectedStatuses(200, 429));

const failed = new Rate('failed'); // not 200 and not 429
const ok200 = new Counter('ok_200');
const limited429 = new Counter('limited_429');
const errors = new Counter('errors_other');
const okHold = new Counter('ok_200_hold'); // 200 answers while all VUs were up

export const options = {
  discardResponseBodies: true,
  noConnectionReuse: false,
  scenarios: {
    load: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: RAMP, target: VUS },
        { duration: HOLD, target: VUS },
        { duration: '10s', target: 0 },
      ],
      gracefulRampDown: '10s',
      gracefulStop: '15s',
    },
  },
  thresholds: {
    failed: [`rate<${MAX_ERROR_RATE}`],
    ok_200: ['count>=0'],
    limited_429: ['count>=0'],
    errors_other: ['count>=0'],
    ok_200_hold: ['count>=0'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  if (VUS < 10000 || holdS < 120) {
    console.warn(`VUS=${VUS} HOLD=${HOLD} is below the spec minimum (10000 VUs for 120 s), fine for a smoke run only`);
  }
  const res = http.post(`${BASE}/auth/login`, JSON.stringify({ email: __ENV.EMAIL, password: __ENV.PASSWORD }), {
    headers: { 'Content-Type': 'application/json' },
    responseType: 'text',
  });
  if (res.status !== 200) throw new Error(`login failed: ${res.status} ${res.body}`);
  const start = Date.now(); // ramp-up begins right after setup
  return {
    cookie: `jwt=${res.cookies.jwt[0].value}`,
    holdFrom: start + rampS * 1000,
    holdTo: start + (rampS + holdS) * 1000,
  };
}

const mix = [
  '/hazards?type=SEISMIC',
  '/hazards?type=VOLCANIC',
  '/hazards?type=SEISMIC&fields=hazard_id,severity,area_name',
  '/hazards?type=VOLCANIC&fields=hazard_id,severity',
];

export default function (data) {
  const res = http.get(`${BASE}${mix[Math.floor(Math.random() * mix.length)]}`, { headers: { Cookie: data.cookie } });
  if (res.status === 200) {
    ok200.add(1);
    const now = Date.now();
    if (now >= data.holdFrom && now < data.holdTo) okHold.add(1);
    failed.add(false);
  } else if (res.status === 429) {
    limited429.add(1);
    failed.add(false);
  } else {
    errors.add(1);
    failed.add(true);
  }
  sleep(THINK_S * (0.8 + Math.random() * 0.4));
}

function val(data, name, key) {
  const m = data.metrics[name];
  return m && m.values && m.values[key] !== undefined ? m.values[key] : 0;
}

export function handleSummary(data) {
  const ok = val(data, 'ok_200', 'count');
  const limited = val(data, 'limited_429', 'count');
  const other = val(data, 'errors_other', 'count');
  const total = ok + limited + other;
  const compact = {
    config: { base_url: BASE, vus: VUS, hold: HOLD, ramp: RAMP, think_s: THINK_S, max_error_rate: MAX_ERROR_RATE },
    requests: total,
    ok_200: ok,
    limited_429: limited,
    errors_other: other,
    error_rate_excluding_429: total ? other / total : 0,
    share_200: total ? ok / total : 0,
    hold_seconds: holdS,
    ok_200_hold: val(data, 'ok_200_hold', 'count'),
    throughput_200_per_s: val(data, 'ok_200_hold', 'count') / holdS,
    vus_max: val(data, 'vus_max', 'max'),
    p95_ms: val(data, 'http_req_duration', 'p(95)'),
    thresholds_ok: Object.values(data.metrics).every((m) => !m.thresholds || Object.values(m.thresholds).every((t) => t.ok)),
  };
  const text =
    `requests=${total} ok_200=${ok} limited_429=${limited} errors_other=${other}\n` +
    `share_200=${(compact.share_200 * 100).toFixed(1)}%${compact.share_200 < 0.95 ? ' WARNING: mostly 429 or errors, this run does not measure the Aggregator' : ''}\n` +
    `error_rate_excluding_429=${(compact.error_rate_excluding_429 * 100).toFixed(3)}% (limit ${MAX_ERROR_RATE * 100}%)\n` +
    `throughput_200_per_s=${compact.throughput_200_per_s.toFixed(1)} over ${holdS}s hold, vus_max=${compact.vus_max}, p95=${compact.p95_ms.toFixed(1)}ms\n`;
  const out = { stdout: text };
  const file = __ENV.SUMMARY_FILE;
  if (file) {
    out[file] = JSON.stringify(data, null, 2);
    out[file.replace(/\.json$/, '') + '.compact.json'] = JSON.stringify(compact, null, 2);
  }
  return out;
}
