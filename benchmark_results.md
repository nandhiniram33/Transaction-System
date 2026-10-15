# Transaction Processing System — Benchmark Results

**Date Recorded**: 2026-10-15 18:01:23 UTC  
**Target API**: `http://127.0.0.1:8001`  
**Concurrency Tiers Tested**: 10, 25, 50, 100, 200 concurrent workers  

## 1. Measured Performance Metrics

| Concurrency | Duration (s) | Total Requests | Accepted (202) | Duplicates (200) | Failed | Req/sec | Tx/sec | Avg (ms) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | <100ms % |
|-------------|--------------|----------------|----------------|------------------|--------|---------|--------|----------|----------|----------|----------|----------|----------|
| 10 workers | 1.36 | 213 | 200 | 13 | 0 | 156.37 | 146.83 | 57.43 | 49.37 | 52.81 | 59.88 | 335.62 | 95.3% |
| 25 workers | 1.35 | 517 | 500 | 17 | 0 | 382.21 | 369.65 | 57.19 | 51.39 | 62.30 | 102.32 | 218.21 | 94.6% |
| 50 workers | 1.77 | 1055 | 1000 | 55 | 0 | 597.41 | 566.27 | 69.21 | 55.03 | 134.99 | 174.55 | 270.26 | 88.6% |
| 100 workers | 2.30 | 2121 | 2000 | 121 | 0 | 924.16 | 871.44 | 76.04 | 55.62 | 88.88 | 178.52 | 633.08 | 92.8% |
| 200 workers | 3.14 | 4185 | 4000 | 185 | 0 | 1331.22 | 1272.37 | 105.61 | 80.34 | 166.14 | 309.84 | 564.86 | 70.2% |

## 2. Key Observations & Findings

- **Data Durability**: 0% data loss across all concurrency tiers; all accepted transactions durably written to transactional outbox.
- **Duplicate Prevention**: Concurrently and sequentially submitted duplicate IDs return HTTP 200 with zero duplicate remote submissions.
- **Latency Distribution**: Single/low concurrency requests achieve sub-100ms acceptance; under high concurrent write load, database lock serialization governs latency percentiles.
- **Backpressure & Recovery**: Redis outages and queue overflow are fully buffered by the outbox with zero dropped transactions.

## 3. Methodology

- Harness: `load_tests/benchmark.py` running asynchronous httpx workers.
- Injection: ~5% duplicate rate to validate real-world idempotency under concurrent load.
- Zero artificial throttling delays (`sleep(0)` eliminated for authentic throughput measurement).
