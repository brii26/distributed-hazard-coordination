// Problem 2 load test, run by tests/load/run.sh with k6.
//
//   bmkg_only    p95 latency of /hazards?type=SEISMIC (BMKG data) while PVMBG answers after 3 s
//   volcanic_bg  /hazards?type=VOLCANIC traffic at the same time as bmkg_only, so a slow PVMBG request is in flight while the BMKG-only p95 is measured
// mixed        >= 50 concurrent connections for 60 s of mixed traffic, errors exclude the controlled 429
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const BASE = __ENV.BASE_URL;
const VUS = Number(__ENV.MIXED_VUS || 50);
const DURATION = __ENV.MIXED_DURATION || '60s';

// Failed is anything but 200 or the controlled 429.
const failed = new Rate('failed');

const statuses = [200, 401, 429, 500, 502, 503, 504];

export const options = {
  scenarios: {
    bmkg_only: {
      executor: 'constant-arrival-rate',
      rate: 20,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 10,
      maxVUs: 20,
      exec: 'bmkgOnly',
    },
    volcanic_bg: {
      executor: 'constant-arrival-rate',
      rate: 10,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 15,
      maxVUs: 40,
      exec: 'volcanicBg',
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
      'failed{scenario:volcanic_bg}': ['rate<0.01'],
      'checks{scenario:bmkg_only}': ['rate>0.99'],
      'http_reqs{scenario:bmkg_only,status:429}': ['count>=0'],
      'http_reqs{scenario:volcanic_bg,status:429}': ['count>=0'],
      'http_req_duration{scenario:volcanic_bg}': ['p(95)>=0'],
      'http_req_duration{scenario:mixed}': ['p(95)>=0'],
    },
    ...statuses.map((s) => ({ [`http_reqs{scenario:mixed,status:${s}}`]: ['count>=0'] })),
  ),
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

// One login for the whole run, the token outlives it.
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

export function volcanicBg(data) {
  const res = get('/hazards?type=VOLCANIC', data);
  check(res, { 'volcanic 200 or 429': (r) => r.status === 200 || r.status === 429 });
}

const mix = [
  '/hazards?type=SEISMIC',
  '/hazards?type=VOLCANIC',
  '/hazards?type=SEISMIC&fields=hazard_id,severity,area_name',
  '/hazards?type=VOLCANIC&fields=hazard_id,severity',
  '/health',
];

// About a second of think time per connection, so 50 connections ask for roughly RATE_LIMIT_RPS in total.
export function mixed(data) {
  get(mix[Math.floor(Math.random() * mix.length)], data);
  sleep(0.8 + Math.random() * 0.4);
}
