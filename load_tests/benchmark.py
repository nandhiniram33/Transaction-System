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
) -> None:
    for i in range(total_per_worker):
        payload = {
            "id": str(uuid.uuid4()),
            "amount": round(random.uniform(5.0, 500.0), 2),
            "currency": random.choice(["USD", "EUR", "GBP", "JPY"]),
            "description": f"Benchmark transaction {i}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metadata": {"batch": "benchmark", "seq": i},
        }

        for kind in ("new", "duplicate") if random.random() < 0.05 else ("new",):
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

        await asyncio.sleep(0.01)


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values.sort()
    return values[min(int(p * (len(values) - 1)), len(values) - 1)]


async def run_benchmark(
    base_url: str = "http://127.0.0.1:8000",
    concurrency: int = 10,
    requests_per_worker: int = 20,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    limits = httpx.Limits(
        max_keepalive_connections=concurrency,
        max_connections=concurrency * 2,
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

    print("\n-------------- BENCHMARK RESULTS --------------")
    print(f"Duration:                  {duration:.2f} seconds")
    print(f"Total HTTP requests:       {len(results)}")
    print(f"New submissions accepted:  {len(successful_new)}")
    print(f"Duplicate responses:       {len(successful_duplicates)}")
    print(f"Failed/non-successful:     {len(results) - len(successful_responses)}")
    print(f"HTTP request throughput:   {len(results) / duration:.2f} req/sec")
    print(f"New-transaction throughput:{len(successful_new) / duration: .2f} tx/sec")
    print(
        f"Successful response rate:  {len(successful_responses) / max(len(results), 1) * 100:.2f}%"
    )
    print(
        f"Average latency:            {statistics.mean(latencies) if latencies else 0:.2f} ms"
    )
    for p in (0.50, 0.90, 0.95, 0.99):
        print(
            f"p{int(p * 100):<2} latency:                 {percentile(latencies.copy(), p):.2f} ms"
        )
    print("-----------------------------------------------")

    return {
        "duration_seconds": round(duration, 2),
        "http_requests": len(results),
        "new_transactions_accepted": len(successful_new),
        "duplicate_responses": len(successful_duplicates),
        "http_requests_per_second": round(len(results) / duration, 2),
        "new_transactions_per_second": round(len(successful_new) / duration, 2),
        "p95_ms": round(percentile(latencies.copy(), 0.95), 2),
    }


if __name__ == "__main__":
    import sys

    url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
    asyncio.run(run_benchmark(url, concurrency=10, requests_per_worker=20))
