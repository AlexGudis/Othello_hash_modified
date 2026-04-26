from __future__ import annotations

import random
from typing import Any


Operation = tuple[str, Any, Any]


class KeyPool:
    def __init__(self) -> None:
        self.live: list[str] = []
        self.next_id = 0

    def make_new_key(self) -> str:
        key = f"k{self.next_id}"
        self.next_id += 1
        return key

    def preload(self, n: int) -> list[Operation]:
        ops: list[Operation] = []
        for _ in range(n):
            key = self.make_new_key()
            self.live.append(key)
            ops.append(("insert", key, key))
        return ops

    def random_hit_key(self) -> str:
        return random.choice(self.live)

    def random_miss_key(self) -> str:
        return f"miss_{random.randrange(10**12)}"

    def consume_delete_key(self) -> str:
        idx = random.randrange(len(self.live))
        key = self.live[idx]
        self.live[idx] = self.live[-1]
        self.live.pop()
        return key


def preload_to_alpha(table_factory, alpha: float) -> list[Operation]:
    table = table_factory()
    target_n = int(alpha * table.capacity())
    pool = KeyPool()
    return pool.preload(target_n)


def lookup_only(table_factory, alpha: float, queries: int, hit_rate: float = 0.8) -> list[Operation]:
    table = table_factory()
    target_n = int(alpha * table.capacity())
    pool = KeyPool()

    ops = pool.preload(target_n)
    for _ in range(queries):
        if pool.live and random.random() < hit_rate:
            key = pool.random_hit_key()
        else:
            key = pool.random_miss_key()
        ops.append(("find", key, None))
    return ops


def mixed_workload(
    table_factory,
    alpha: float,
    operations: int,
    p_find: float = 0.80,
    p_insert: float = 0.15,
    p_delete: float = 0.05,
    hit_rate: float = 0.8,
) -> list[Operation]:
    if round(p_find + p_insert + p_delete, 10) != 1.0:
        raise ValueError("Probabilities must sum to 1.0")

    table = table_factory()
    target_n = int(alpha * table.capacity())
    pool = KeyPool()
    ops = pool.preload(target_n)

    for _ in range(operations):
        r = random.random()
        if r < p_find:
            if pool.live and random.random() < hit_rate:
                key = pool.random_hit_key()
            else:
                key = pool.random_miss_key()
            ops.append(("find", key, None))
        elif r < p_find + p_insert:
            key = pool.make_new_key()
            pool.live.append(key)
            ops.append(("insert", key, key))
        else:
            if pool.live:
                key = pool.consume_delete_key()
                ops.append(("delete", key, None))
            else:
                ops.append(("find", "empty", None))

    return ops


def load_sweep_alphas(start: float = 0.05, stop: float = 0.50, step: float = 0.05) -> list[float]:
    alphas: list[float] = []
    x = start
    while x <= stop + 1e-12:
        alphas.append(round(x, 4))
        x += step
    return alphas
