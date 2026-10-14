import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

// Custom metrics
const submissionLatency = new Trend('transaction_submission_latency');
const duplicateCounter = new Counter('duplicate_submissions');
const conflictCounter = new Counter('conflict_submissions');

export const options = {
  stages: [
    { duration: '10s', target: 50 },    // Ramp up to 50 virtual users
    { duration: '30s', target: 200 },   // Spike to 200 virtual users
    { duration: '20s', target: 500 },   // Stress test with 500 VUs targeting 1000+ TPS
    { duration: '10s', target: 0 },     // Ramp down
  ],
  thresholds: {
    // 95% of submission requests must complete within 100ms as per strict PDF SLA
    http_req_duration: ['p(95)<100'],
    http_req_failed: ['rate<0.01'], // < 1% error rate
  },
};

const BASE_URL = __ENV.API_BASE_URL || 'http://localhost:8000';

function generateUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

export default function () {
  const txId = generateUUID();
  const payload = JSON.stringify({
    id: txId,
    amount: (Math.random() * 1000 + 1).toFixed(2),
    currency: 'USD',
    description: 'High performance load test transaction',
    timestamp: new Date().toISOString(),
    metadata: {
      vu: __VU,
      iter: __ITER,
    },
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  // 1. Initial Submission
  const startTime = new Date();
  const res = http.post(`${BASE_URL}/api/transactions`, payload, params);
  const latency = new Date() - startTime;
  submissionLatency.add(latency);

  check(res, {
    'submission is 202 Accepted': (r) => r.status === 202,
    'latency is under 100ms': () => latency < 100,
  });

  // 2. 5% of requests test identical duplicate submission
  if (Math.random() < 0.05) {
    const dupRes = http.post(`${BASE_URL}/api/transactions`, payload, params);
    duplicateCounter.add(1);
    check(dupRes, {
      'identical duplicate is 200 OK': (r) => r.status === 200,
    });
  }

  // 3. 1% of requests test conflicting submission with altered amount
  if (Math.random() < 0.01) {
    const conflictPayload = JSON.stringify({
      id: txId,
      amount: '99999.99',
      currency: 'EUR',
      description: 'Conflict payload',
      timestamp: new Date().toISOString(),
    });
    const conflictRes = http.post(`${BASE_URL}/api/transactions`, conflictPayload, params);
    conflictCounter.add(1);
    check(conflictRes, {
      'conflicting submission is 409 Conflict': (r) => r.status === 409,
    });
  }

  sleep(0.01);
}
