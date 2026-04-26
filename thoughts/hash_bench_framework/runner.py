from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from time import perf_counter_ns
from typing import Any, Callable, Iterable, Optional

from metrics import build_latency_stats, diff_metrics


Operation = tuple[str, Any, Any]


@dataclass
class BenchmarkResult:
    algorithm: str
    workload: str
    repeat: int
    n: int
    capacity: int
    load_factor: float
    insert_avg_ns: float
    insert_p50_ns: float
    insert_p95_ns: float
    insert_p99_ns: float
    insert_max_ns: float
    insert_count: int
    find_avg_ns: float
    find_p50_ns: float
    find_p95_ns: float
    find_p99_ns: float
    find_max_ns: float
    find_count: int
    delete_avg_ns: float
    delete_p50_ns: float
    delete_p95_ns: float
    delete_p99_ns: float
    delete_max_ns: float
    delete_count: int
    ops_total: int
    runtime_ns: int
    throughput_ops_s: float
    memory_probe: int
    memory_write: int
    collision: int
    relocation: int
    resize: int
    cycle_detected: int
    hash_calls_total: int
    hash_calls_h1: int
    hash_calls_h2: int
    delete_success: int
    delete_miss: int
    duplicate_insert: int
    insert_success: int
    approx_memory_bytes: int


class BenchmarkRunner:
    def run_ops(
        self,
        table,
        ops: Iterable[Operation],
        *,
        algorithm_name: str,
        workload_name: str,
        repeat: int = 0,
        memory_estimator: Optional[Callable[[Any], int]] = None,
    ) -> BenchmarkResult:
        insert_lat: list[int] = []
        find_lat: list[int] = []
        delete_lat: list[int] = []

        table.reset_metrics()
        before = table.metrics_snapshot()
        total_ops = 0
        t_run = perf_counter_ns()

        for op, key, value in ops:
            total_ops += 1
            t0 = perf_counter_ns()

            if op == "insert":
                table.insert(key, value)
                insert_lat.append(perf_counter_ns() - t0)
            elif op == "find":
                table.find(key)
                find_lat.append(perf_counter_ns() - t0)
            elif op == "delete":
                table.delete(key)
                delete_lat.append(perf_counter_ns() - t0)
            else:
                raise ValueError(f"Unsupported operation: {op}")

        runtime_ns = perf_counter_ns() - t_run
        after = table.metrics_snapshot()
        delta = diff_metrics(before, after)

        insert_stats = build_latency_stats(insert_lat)
        find_stats = build_latency_stats(find_lat)
        delete_stats = build_latency_stats(delete_lat)
        approx_memory = memory_estimator(table) if memory_estimator else 0

        return BenchmarkResult(
            algorithm=algorithm_name,
            workload=workload_name,
            repeat=repeat,
            n=len(table),
            capacity=table.capacity(),
            load_factor=table.load_factor(),
            insert_avg_ns=insert_stats.avg_ns,
            insert_p50_ns=insert_stats.p50_ns,
            insert_p95_ns=insert_stats.p95_ns,
            insert_p99_ns=insert_stats.p99_ns,
            insert_max_ns=insert_stats.max_ns,
            insert_count=insert_stats.count,
            find_avg_ns=find_stats.avg_ns,
            find_p50_ns=find_stats.p50_ns,
            find_p95_ns=find_stats.p95_ns,
            find_p99_ns=find_stats.p99_ns,
            find_max_ns=find_stats.max_ns,
            find_count=find_stats.count,
            delete_avg_ns=delete_stats.avg_ns,
            delete_p50_ns=delete_stats.p50_ns,
            delete_p95_ns=delete_stats.p95_ns,
            delete_p99_ns=delete_stats.p99_ns,
            delete_max_ns=delete_stats.max_ns,
            delete_count=delete_stats.count,
            ops_total=total_ops,
            runtime_ns=runtime_ns,
            throughput_ops_s=0.0 if runtime_ns == 0 else total_ops / (runtime_ns / 1e9),
            memory_probe=delta.get("memory_probe", 0),
            memory_write=delta.get("memory_write", 0),
            collision=delta.get("collision", 0),
            relocation=delta.get("relocation", 0),
            resize=delta.get("resize", 0),
            cycle_detected=delta.get("cycle_detected", 0),
            hash_calls_total=delta.get("hash_calls_total", 0),
            hash_calls_h1=delta.get("hash_calls_h1", 0),
            hash_calls_h2=delta.get("hash_calls_h2", 0),
            delete_success=delta.get("delete_success", 0),
            delete_miss=delta.get("delete_miss", 0),
            duplicate_insert=delta.get("duplicate_insert", 0),
            insert_success=delta.get("insert_success", 0),
            approx_memory_bytes=approx_memory,
        )


def save_results_csv(results: list[BenchmarkResult], path: str) -> None:
    if not results:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(results[0]).keys()))
        writer.writeheader()
        for row in results:
            writer.writerow(asdict(row))
