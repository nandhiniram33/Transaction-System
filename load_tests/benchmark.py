"""High-Performance Load and Stress Benchmark for Transaction Submission API."""

import asyncio
from datetime import datetime, timezone
import random
import statistics
import time
import uuid
from typing import Any, Dict, List
import httpx


async def worker_task(
    client: httpx.AsyncClient,
    base_url: str,
    total_per_worker: int,
    results: List[Dict[str, Any]],
):
    """Execute transaction submissions and record latency."""
    for i in range(total_per_worker):
        tx_id = str(uuid.uuid4())
        payload = {
            "id": tx_id,
            "amount": round(random.uniform(5.0, 500.0), 2),
            "currency": random.choice(["USD", "EUR", "GBP", "JPY"]),
            "description": f"Benchmark transaction {i}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metadata": {"batch": "benchmark", "seq": i},
        }

        # 1. Primary Submission
        t0 = time.perf_counter()
        try:
            resp = await client.post(f"{base_url}/api/transactions", json=payload)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            results.append({
                "status_code": resp.status_code,
                "latency_ms": elapsed_ms,
                "type": "new",
            })
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            results.append({
                "status_code": 0,
                "latency_ms": elapsed_ms,
                "error": str(exc),
                "type": "error",
            })

        # 2. 5% probability of testing identical duplicate
        if random.random() < 0.05:
            t0 = time.perf_counter()
            try:
                resp = await client.post(f"{base_url}/api/transactions", json=payload)
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                results.append({
                    "status_code": resp.status_code,
                    "latency_ms": elapsed_ms,
                    "type": "duplicate",
                })
            except Exception as exc:
                results.append({"status_code": 0, "latency_ms": 0, "error": str(exc), "type": "error"})

        # Short realistic pacing (10ms) between transaction submissions
        await asyncio.sleep(0.01)


async def run_benchmark(
    base_url: str = "http://127.0.0.1:8000",
    concurrency: int = 50,
    requests_per_worker: int = 40,
):
    """Run concurrent load test against the API."""
    total_expected = concurrency * requests_per_worker
    print("\n=======================================================")
    print(f" Starting Benchmark: {total_expected} transactions across {concurrency} workers")
    print(f" Target Endpoint: {base_url}/api/transactions")
    print("=======================================================\n")

    limits = httpx.Limits(max_keepalive_connections=concurrency, max_connections=concurrency * 2)
    timeout = httpx.Timeout(10.0, connect=5.0)
    results: List[Dict[str, Any]] = []

    start_bench = time.perf_counter()

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        tasks = [
            worker_task(client, base_url, requests_per_worker, results)
            for _ in range(concurrency)
        ]
        await asyncio.gather(*tasks)

    total_duration = time.perf_counter() - start_bench

    # Analyze results
    latencies = [r["latency_ms"] for r in results if r["status_code"] in (200, 202)]
    status_counts = {}
    for r in results:
        code = r["status_code"]
        status_counts[code] = status_counts.get(code, 0) + 1

    total_reqs = len(results)
    successful = status_counts.get(202, 0) + status_counts.get(200, 0)
    tps = total_reqs / total_duration

    sorted_lat = sorted(latencies) if latencies else [0]
    n = len(sorted_lat)

    p50 = sorted_lat[int(0.50 * (n - 1))]
    p90 = sorted_lat[int(0.90 * (n - 1))]
    p95 = sorted_lat[int(0.95 * (n - 1))]
    p99 = sorted_lat[int(0.99 * (n - 1))]
    avg_lat = statistics.mean(latencies) if latencies else 0.0

    print("------------------ BENCHMARK RESULTS ------------------")
    print(f" Total Requests Completed:  {total_reqs}")
    print(f" Total Duration:            {total_duration:.2f} seconds")
    print(f" Throughput (TPS):          {tps:.2f} req/sec")
    print(f" Success Rate:              {(successful / total_reqs * 100):.2f}%")
    print(f" Status Breakdown:          {status_counts}")
    print(" Latency Distribution (ms):")
    print(f"   - Average:       {avg_lat:.2f} ms")
    print(f"   - Median (p50):  {p50:.2f} ms")
    print(f"   - p90:           {p90:.2f} ms")
    print(f"   - p95:           {p95:.2f} ms")
    print(f"   - p99:           {p99:.2f} ms")
    sla_met = p95 < 100.0 and (successful / total_reqs >= 0.99)
    print(" SLA Assessment:")
    print(f"   - Sub-100ms Submission SLA (p95 < 100ms): {'[PASSED - 100% SLA MET]' if sla_met else '[FAILED]'}")
    print("-------------------------------------------------------\n")
    return {
        "total_requests": total_reqs,
        "duration_seconds": round(total_duration, 2),
        "tps": round(tps, 2),
        "status_counts": status_counts,
        "sla_met": sla_met,
        "latency_ms": {
            "avg": round(avg_lat, 2),
            "p50": round(p50, 2),
            "p90": round(p90, 2),
            "p95": round(p95, 2),
            "p99": round(p99, 2),
        },
    }


if __name__ == "__main__":
    import sys
    url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
    asyncio.run(run_benchmark(base_url=url, concurrency=10, requests_per_worker=20))

