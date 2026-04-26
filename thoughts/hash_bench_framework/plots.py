from __future__ import annotations

import os
from typing import Iterable

import matplotlib.pyplot as plt
import pandas as pd


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def plot_metric_lines(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    by: str,
    title: str,
    xlabel: str,
    ylabel: str,
    output_path: str,
) -> None:
    plt.figure(figsize=(8, 4.5))
    for name, part in df.groupby(by):
        part = part.sort_values(x)
        plt.plot(part[x], part[y], marker="o", label=str(name))
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def plot_bar(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    title: str,
    xlabel: str,
    ylabel: str,
    output_path: str,
) -> None:
    plt.figure(figsize=(8, 4.5))
    plt.bar(df[x].astype(str), df[y])
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def plot_histogram(
    values: Iterable[float],
    *,
    bins: int,
    title: str,
    xlabel: str,
    ylabel: str,
    output_path: str,
) -> None:
    plt.figure(figsize=(8, 4.5))
    plt.hist(list(values), bins=bins)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def generate_default_plots(df: pd.DataFrame, outdir: str) -> None:
    ensure_dir(outdir)

    if "workload" in df.columns and df["workload"].nunique() > 1:
        plot_bar(
            df.groupby("workload", as_index=False)["throughput_ops_s"].mean(),
            x="workload",
            y="throughput_ops_s",
            title="Throughput by workload",
            xlabel="workload",
            ylabel="ops / s",
            output_path=os.path.join(outdir, "throughput_by_workload.png"),
        )

    if "workload" in df.columns and "load_factor" in df.columns:
        plot_metric_lines(
            df.groupby(["algorithm", "workload"], as_index=False)["find_p95_ns"].mean(),
            x="workload",
            y="find_p95_ns",
            by="algorithm",
            title="p95 lookup latency by workload",
            xlabel="workload",
            ylabel="ns",
            output_path=os.path.join(outdir, "find_p95_by_workload.png"),
        )

        plot_metric_lines(
            df.groupby(["algorithm", "workload"], as_index=False)["hash_calls_total"].mean(),
            x="workload",
            y="hash_calls_total",
            by="algorithm",
            title="Hash calls by workload",
            xlabel="workload",
            ylabel="count",
            output_path=os.path.join(outdir, "hash_calls_by_workload.png"),
        )

        plot_metric_lines(
            df.groupby(["algorithm", "workload"], as_index=False)["memory_probe"].mean(),
            x="workload",
            y="memory_probe",
            by="algorithm",
            title="Memory probes by workload",
            xlabel="workload",
            ylabel="count",
            output_path=os.path.join(outdir, "memory_probes_by_workload.png"),
        )
