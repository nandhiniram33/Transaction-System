"""High-concurrency async benchmark for transaction ingestion performance measurement.

Measures throughput (req/sec), sub-100ms submission latency compliance,
and error rate under realistic concurrent load without artificial throttling.
"""

import asyncio
from datetime import datetime, timezone
import random
import statistics
import time
import uuid
from typing import Any
import httpx


async def worker_task(
    client: httpx.AsyncClient,
    base_url: str,
    total_per_worker: int,
    results: list[dict[str, Any]],
    include_duplicates: bool = True,
) -> None:
    """Submit transactions concurrently without artificial throttling delays."""
    for i in range(total_per_worker):
        payload = {
            "id": str(uuid.uuid4()),
            "amount": round(random.uniform(5.0, 500.0), 2),
            "currency": random.choice(["USD", "EUR", "GBP", "JPY"]),
            "description": f"Benchmark transaction {i}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metadata": {"batch": "benchmark", "seq": i},
        }

        # ~5% duplicate injection to test idempotency
        kinds = (
            ("new", "duplicate")
            if (include_duplicates and random.random() < 0.05)
            else ("new",)
        )

        for kind in kinds:
            t0 = time.perf_counter()
            try:
                response = await client.post(
                    f"{base_url}/api/transactions", json=payload
                )
                elapsed = (time.perf_counter() - t0) * 1000
                results.append(
                    {
                        "type": kind,
                        "status_code": response.status_code,
                        "latency_ms": elapsed,
                    }
                )
            except Exception as exc:
                elapsed = (time.perf_counter() - t0) * 1000
                results.append(
                    {
                        "type": kind,
                        "status_code": 0,
                        "latency_ms": elapsed,
                        "error": str(exc),
                    }
                )
        # No artificial sleep — measure raw system throughput


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values.sort()
    return values[min(int(p * (len(values) - 1)), len(values) - 1)]


async def run_benchmark(
    base_url: str = "http://127.0.0.1:8000",
    concurrency: int = 50,
    requests_per_worker: int = 20,
) -> dict[str, Any]:
    """Run concurrent load test with no artificial throttling."""
    results: list[dict[str, Any]] = []
    limits = httpx.Limits(
        max_keepalive_connections=concurrency,
        max_connections=concurrency * 2,
    )

    print(
        f"\nStarting benchmark: {concurrency} concurrent workers × "
        f"{requests_per_worker} requests = {concurrency * requests_per_worker} total"
    )

    start = time.perf_counter()
    async with httpx.AsyncClient(
        limits=limits, timeout=httpx.Timeout(30.0, connect=5.0)
    ) as client:
        await asyncio.gather(
            *[
                worker_task(client, base_url, requests_per_worker, results)
                for _ in range(concurrency)
            ]
        )
    duration = time.perf_counter() - start

    new_results = [r for r in results if r["type"] == "new"]
    duplicates = [r for r in results if r["type"] == "duplicate"]
    successful_new = [r for r in new_results if r["status_code"] == 202]
    successful_duplicates = [r for r in duplicates if r["status_code"] == 200]
    successful_responses = [r for r in results if r["status_code"] in (200, 202)]
    latencies = [r["latency_ms"] for r in successful_responses]
    sub_100ms = [lat for lat in latencies if lat < 100.0]

    print("\n-------------- BENCHMARK RESULTS --------------")
    print(f"Duration:                  {duration:.2f} seconds")
    print(f"Concurrency:               {concurrency} workers")
    print(f"Total HTTP requests:       {len(results)}")
    print(f"New submissions accepted:  {len(successful_new)}")
    print(f"Duplicate responses:       {len(successful_duplicates)}")
    print(f"Failed/non-successful:     {len(results) - len(successful_responses)}")
    print(f"HTTP request throughput:   {len(results) / duration:.2f} req/sec")
    print(f"New-transaction throughput:{len(successful_new) / duration: .2f} tx/sec")
    print(
        f"Successful response rate:  {len(successful_responses) / max(len(results), 1) * 100:.2f}%"
    )
    if latencies:
        print(f"Average latency:           {statistics.mean(latencies):.2f} ms")
        for p in (0.50, 0.90, 0.95, 0.99):
            mark = " [OK <100ms]" if percentile(latencies.copy(), p) < 100 else ""
            print(
                f"p{int(p * 100):<2} latency:              {percentile(latencies.copy(), p):.2f} ms{mark}"
            )
        sub_pct = len(sub_100ms) / len(latencies) * 100
        print(
            f"Requests <100ms:           {len(sub_100ms)}/{len(latencies)} ({sub_pct:.1f}%)"
        )
    print("-----------------------------------------------")

    return {
        "duration_seconds": round(duration, 2),
        "concurrency": concurrency,
        "http_requests": len(results),
        "new_transactions_accepted": len(successful_new),
        "duplicate_responses": len(successful_duplicates),
        "failed": len(results) - len(successful_responses),
        "http_requests_per_second": round(len(results) / duration, 2),
        "new_transactions_per_second": round(len(successful_new) / duration, 2),
        "avg_ms": round(statistics.mean(latencies), 2) if latencies else 0,
        "p50_ms": round(percentile(latencies.copy(), 0.50), 2),
        "p90_ms": round(percentile(latencies.copy(), 0.90), 2),
        "p95_ms": round(percentile(latencies.copy(), 0.95), 2),
        "p99_ms": round(percentile(latencies.copy(), 0.99), 2),
        "sub_100ms_pct": round(len(sub_100ms) / max(len(latencies), 1) * 100, 1),
    }


if __name__ == "__main__":
    import sys

    url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
    concurrency = int(sys.argv[2]) if len(sys.argv) > 2 else 50
    per_worker = int(sys.argv[3]) if len(sys.argv) > 3 else 20
    asyncio.run(
        run_benchmark(url, concurrency=concurrency, requests_per_worker=per_worker)
    )
