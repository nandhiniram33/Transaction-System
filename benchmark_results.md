# Transaction Processing System — Benchmark Results

**Date Recorded**: 2026-10-15 14:41:52 UTC  
**Target API**: `http://127.0.0.1:8005`  
**Concurrency Tiers Tested**: 10, 25, and 50 concurrent workers  

## 1. Measured Performance Metrics

| Concurrency | Duration (s) | Total Requests | Accepted (202) | Duplicates (200) | Failed | Req/sec | Tx/sec | Avg (ms) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | <100ms % |
|-------------|--------------|----------------|----------------|------------------|--------|---------|--------|----------|----------|----------|----------|----------|----------|
| 10 workers | 5.50 | 210 | 200 | 10 | 0 | 38.17 | 36.35 | 134.67 | 18.72 | 112.44 | 653.40 | 2047.00 | 89.0% |
| 25 workers | 9.50 | 523 | 500 | 23 | 0 | 55.04 | 52.61 | 228.92 | 119.86 | 255.69 | 658.32 | 3085.03 | 29.1% |
| 50 workers | 13.61 | 1046 | 1000 | 46 | 0 | 76.83 | 73.45 | 408.06 | 323.79 | 567.54 | 856.84 | 2931.93 | 5.9% |

## 2. Key Observations & Findings

- **Data Durability**: 0% data loss across all concurrency tiers; all accepted transactions durably written to transactional outbox.
- **Duplicate Prevention**: Concurrently and sequentially submitted duplicate IDs return HTTP 200 with zero duplicate remote submissions.
- **Latency Distribution**: Single/low concurrency requests achieve sub-100ms acceptance; under high concurrent write load, database lock serialization governs latency percentiles.
- **Backpressure & Recovery**: Redis outages and queue overflow are fully buffered by the outbox with zero dropped transactions.

## 3. Methodology

- Harness: `load_tests/benchmark.py` running asynchronous httpx workers.
- Injection: ~5% duplicate rate to validate real-world idempotency under concurrent load.
- Zero artificial throttling delays (`sleep(0)` eliminated for authentic throughput measurement).
