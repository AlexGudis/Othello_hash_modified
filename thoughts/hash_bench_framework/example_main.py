from __future__ import annotations

import os
import random
import sys

import pandas as pd

from algorithms.cuckoo import CuckooHash
from plots import ensure_dir, generate_default_plots, plot_metric_lines
from runner import BenchmarkRunner, save_results_csv
from workloads import lookup_only, mixed_workload


def approx_memory_bytes(table) -> int:
    size = sys.getsizeof(table)
    size += sys.getsizeof(table.t1) + sys.getsizeof(table.t2)
    for bucket in table.t1:
        if bucket is not None:
            size += sys.getsizeof(bucket)
            size += sys.getsizeof(bucket[0])
            size += sys.getsizeof(bucket[1])
    for bucket in table.t2:
        if bucket is not None:
            size += sys.getsizeof(bucket)
            size += sys.getsizeof(bucket[0])
            size += sys.getsizeof(bucket[1])
    return size


def cuckoo_factory(m: int = 4096):
    def build():
        return CuckooHash(m=m)
    build.__name__ = f"CuckooHash(m={m})"
    return build


def run_demo() -> None:
    random.seed(42)
    outdir = "results"
    ensure_dir(outdir)

    runner = BenchmarkRunner()
    results = []

    algorithms = [
        cuckoo_factory(1024),
        cuckoo_factory(4096),
    ]

    workload_specs = [
        ("lookup_alpha_0.20", lambda factory: lookup_only(factory, alpha=0.20, queries=10000)),
        ("lookup_alpha_0.40", lambda factory: lookup_only(factory, alpha=0.40, queries=10000)),
        (
            "mixed_alpha_0.35",
            lambda factory: mixed_workload(factory, alpha=0.35, operations=20000, p_find=0.80, p_insert=0.15, p_delete=0.05),
        ),
        (
            "mixed_alpha_0.45",
            lambda factory: mixed_workload(factory, alpha=0.45, operations=20000, p_find=0.70, p_insert=0.20, p_delete=0.10),
        ),
    ]

    repeats = 3

    for factory in algorithms:
        for workload_name, workload_builder in workload_specs:
            for repeat in range(repeats):
                table = factory()
                ops = workload_builder(factory)
                result = runner.run_ops(
                    table,
                    ops,
                    algorithm_name=factory.__name__,
                    workload_name=workload_name,
                    repeat=repeat,
                    memory_estimator=approx_memory_bytes,
                )
                results.append(result)

    csv_path = os.path.join(outdir, "benchmark_results.csv")
    save_results_csv(results, csv_path)

    df = pd.read_csv(csv_path)
    summary = (
        df.groupby(["algorithm", "workload"], as_index=False)
        .agg(
            throughput_ops_s=("throughput_ops_s", "mean"),
            find_p95_ns=("find_p95_ns", "mean"),
            insert_p95_ns=("insert_p95_ns", "mean"),
            hash_calls_total=("hash_calls_total", "mean"),
            memory_probe=("memory_probe", "mean"),
            relocation=("relocation", "mean"),
            approx_memory_bytes=("approx_memory_bytes", "mean"),
            load_factor=("load_factor", "mean"),
        )
    )
    summary_path = os.path.join(outdir, "benchmark_summary.csv")
    summary.to_csv(summary_path, index=False)

    plots_dir = os.path.join(outdir, "plots")
    generate_default_plots(df, plots_dir)

    plot_metric_lines(
        summary,
        x="workload",
        y="throughput_ops_s",
        by="algorithm",
        title="Throughput by workload and algorithm",
        xlabel="workload",
        ylabel="ops / s",
        output_path=os.path.join(plots_dir, "throughput_compare.png"),
    )
    plot_metric_lines(
        summary,
        x="workload",
        y="relocation",
        by="algorithm",
        title="Relocations by workload and algorithm",
        xlabel="workload",
        ylabel="count",
        output_path=os.path.join(plots_dir, "relocations_compare.png"),
    )
    plot_metric_lines(
        summary,
        x="workload",
        y="approx_memory_bytes",
        by="algorithm",
        title="Approximate Python memory by workload",
        xlabel="workload",
        ylabel="bytes",
        output_path=os.path.join(plots_dir, "memory_compare.png"),
    )

    print(f"Saved raw results to: {csv_path}")
    print(f"Saved summary to:     {summary_path}")
    print(f"Saved plots to:       {plots_dir}")


if __name__ == "__main__":
    run_demo()
