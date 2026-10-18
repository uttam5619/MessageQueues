import http from 'k6/http';
import { check } from 'k6';

const rate = Number(__ENV.LOAD_TEST_RATE || 10);
const duration = __ENV.LOAD_TEST_DURATION || '30s';
const preAllocatedVUs = Number(__ENV.LOAD_TEST_PRE_ALLOCATED_VUS || 10);
const maxVUs = Number(__ENV.LOAD_TEST_MAX_VUS || 100);

export const options = {
  scenarios: {
    post_load: {
      executor: 'constant-arrival-rate',
      rate,
      timeUnit: '1s',
      duration,
      preAllocatedVUs,
      maxVUs,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<1000'],
  },
};

const endpoint = __ENV.TARGET_URL || 'http://host.docker.internal:3040/kafka/post';

export default function () {
  const payload = JSON.stringify({
    title: `k6 post ${__VU}-${__ITER}`,
    description: 'Load-test message',
  });

  const response = http.post(endpoint, payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(response, {
    'accepted (202)': (r) => r.status === 202,
  });
}
