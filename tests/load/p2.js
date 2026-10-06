// Problem 2 load test (doc/TRD_M1.md 12): run by tests/load/run.sh with k6.
//
//   bmkg_only  p95 latency of /hazards?type=SEISMIC (BMKG data) while PVMBG answers after 3 s
//   mixed      >= 50 concurrent connections for 60 s over a mixed workload; the error
//              rate leaves out 429, which is the controlled load shedding
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const BASE = __ENV.BASE_URL;
const VUS = Number(__ENV.MIXED_VUS || 50);
const DURATION = __ENV.MIXED_DURATION || '60s';

// Failed = anything but 200 or the controlled 429.
const failed = new Rate('failed');

const statuses = [200, 401, 429, 500, 502, 503, 504];

export const options = {
  scenarios: {
    bmkg_only: {
      executor: 'constant-arrival-rate',
      rate: 20, // under RATE_LIMIT_RPS, so this measures latency, not the limiter
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 10,
      maxVUs: 20,
      exec: 'bmkgOnly',
    },
    mixed: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      startTime: '35s',
      exec: 'mixed',
    },
  },
  thresholds: Object.assign(
    {
      'http_req_duration{scenario:bmkg_only}': ['p(95)<300'],
      'failed{scenario:mixed}': ['rate<0.01'],
      'failed{scenario:bmkg_only}': ['rate<0.01'],
    },
    // Empty thresholds only make k6 list a count per status code in the summary.
    ...statuses.map((s) => ({ [`http_reqs{scenario:mixed,status:${s}}`]: ['count>=0'] })),
  ),
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

// One login for the whole run; the token outlives it (run.sh sets ACCESS_TOKEN_TTL_S).
export function setup() {
  const res = http.post(`${BASE}/auth/login`, JSON.stringify({ email: __ENV.EMAIL, password: __ENV.PASSWORD }), {
    headers: { 'Content-Type': 'application/json' },
  });
  if (res.status !== 200) throw new Error(`login failed: ${res.status} ${res.body}`);
  return { cookie: `jwt=${res.cookies.jwt[0].value}` };
}

function get(path, data) {
  const res = http.get(`${BASE}${path}`, { headers: { Cookie: data.cookie } });
  failed.add(res.status !== 200 && res.status !== 429);
  return res;
}

export function bmkgOnly(data) {
  const res = get('/hazards?type=SEISMIC', data);
  check(res, { 'seismic 200': (r) => r.status === 200 });
}

const mix = [
  '/hazards?type=SEISMIC',
  '/hazards?type=VOLCANIC',
  '/hazards?type=SEISMIC&fields=hazard_id,severity,area_name',
  '/hazards?type=VOLCANIC&fields=hazard_id,severity',
  '/health',
];

// About a second of think time per connection: 50 connections then ask for roughly
// RATE_LIMIT_RPS (50) in total, so the limiter sheds only the excess.
export function mixed(data) {
  get(mix[Math.floor(Math.random() * mix.length)], data);
  sleep(0.8 + Math.random() * 0.4);
}
