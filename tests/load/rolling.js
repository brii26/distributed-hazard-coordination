// Constant load for the rolling update scenario, run by tests/scenarios/rolling-update.sh.
// Failed is anything but 200 or the controlled 429, which is only counted.
// One login in setup and the jwt cookie only: rolling-update.sh raises ACCESS_TOKEN_TTL_S so it outlives the run. A refresh would rotate the
// refresh_token, so VUs could not share it, and logging in per VU would hit the per-IP login limit.
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL;

const total = new Counter('rolling_total');
const ok = new Counter('rolling_ok');
const failed = new Counter('rolling_failed');
const throttled = new Counter('rolling_throttled_429');

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 20),
      duration: __ENV.DURATION || '60s',
    },
  },
};

export function setup() {
  const res = http.post(`${BASE}/auth/login`, JSON.stringify({ email: __ENV.EMAIL, password: __ENV.PASSWORD }), {
    headers: { 'Content-Type': 'application/json' },
  });
  if (res.status !== 200) throw new Error(`login failed: ${res.status} ${res.body}`);
  return { cookie: `jwt=${res.cookies.jwt[0].value}` };
}

const paths = ['/hazards?type=SEISMIC', '/hazards?type=VOLCANIC', '/hazards?type=SEISMIC&fields=hazard_id,severity'];

export default function (data) {
  const res = http.get(`${BASE}${paths[Math.floor(Math.random() * paths.length)]}`, { headers: { Cookie: data.cookie } });
  total.add(1);
  if (res.status === 200) ok.add(1);
  else if (res.status === 429) throttled.add(1);
  else if (res.status !== 200) failed.add(1);
  sleep(0.2 + Math.random() * 0.2);
}
