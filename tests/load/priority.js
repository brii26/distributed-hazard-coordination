import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

// Config file is produced by the isolated fixture. Environment tokens allow the
// same workload against a deployed API with real Media and Lapangan accounts.
const config = __ENV.CONFIG_FILE ? JSON.parse(open(__ENV.CONFIG_FILE)) : {
  base_url: __ENV.BASE_URL,
  media_jwt: __ENV.MEDIA_JWT,
  field_jwt: __ENV.FIELD_JWT,
};
const fieldSuccess = new Rate('field_success');
const fieldErrors = new Rate('field_errors_excluding_429');
const mediaErrors = new Rate('media_errors_excluding_429');
const media429 = new Counter('media_429');
const badRetry = new Counter('invalid_retry_after');

export const options = {
  scenarios: {
    media: { executor: 'constant-vus', vus: Number(__ENV.MEDIA_VUS || 40), duration: __ENV.DURATION || '30s', exec: 'media' },
    field: { executor: 'constant-vus', vus: Number(__ENV.FIELD_VUS || 4), duration: __ENV.DURATION || '30s', exec: 'field' },
  },
  thresholds: {
    field_success: ['rate>=0.99'],
    field_errors_excluding_429: ['rate<0.01'],
    media_errors_excluding_429: ['rate<0.01'],
    media_429: ['count>0'],
    invalid_retry_after: ['count==0'],
  },
};

export function setup() {
  if (!config.base_url || !config.media_jwt || !config.field_jwt) throw new Error('BASE_URL, MEDIA_JWT and FIELD_JWT are required');
}

function request(token) {
  const res = http.get(`${config.base_url}/hazards?type=SEISMIC`, {
    headers: { Cookie: `jwt=${token}` },
    responseCallback: http.expectedStatuses(200, 429),
  });
  badRetry.add(res.status === 429 && !(Number(res.headers['Retry-After']) > 0) ? 1 : 0);
  return res;
}

export function media() {
  const res = request(config.media_jwt);
  mediaErrors.add(res.status !== 200 && res.status !== 429);
  media429.add(res.status === 429 ? 1 : 0);
  // Bounded retry rate: pressure stays above capacity without a tight 429 loop.
  sleep(0.01);
}

export function field() {
  const res = request(config.field_jwt);
  fieldSuccess.add(res.status === 200);
  fieldErrors.add(res.status !== 200 && res.status !== 429);
}
