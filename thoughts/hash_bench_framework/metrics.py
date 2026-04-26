from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import DefaultDict


class MetricsObserver:
    """Simple event counter shared by all hash-table implementations."""

    def __init__(self) -> None:
        self._counters: DefaultDict[str, int] = defaultdict(int)

    def event(self, name: str, value: int = 1) -> None:
        self._counters[name] += value

    def snapshot(self) -> dict[str, int]:
        return dict(self._counters)

    def reset(self) -> None:
        self._counters.clear()


@dataclass
class LatencyStats:
    avg_ns: float
    p50_ns: float
    p95_ns: float
    p99_ns: float
    max_ns: float
    count: int


def percentile(values: list[int], p: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    k = (len(xs) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    if f == c:
        return float(xs[f])
    return float(xs[f] + (xs[c] - xs[f]) * (k - f))


def build_latency_stats(values: list[int]) -> LatencyStats:
    if not values:
        return LatencyStats(0.0, 0.0, 0.0, 0.0, 0.0, 0)
    return LatencyStats(
        avg_ns=sum(values) / len(values),
        p50_ns=percentile(values, 50),
        p95_ns=percentile(values, 95),
        p99_ns=percentile(values, 99),
        max_ns=float(max(values)),
        count=len(values),
    )


def diff_metrics(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    keys = set(before) | set(after)
    return {key: after.get(key, 0) - before.get(key, 0) for key in keys}
