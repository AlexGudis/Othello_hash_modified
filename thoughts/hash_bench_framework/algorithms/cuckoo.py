from __future__ import annotations

from typing import Any, Optional

from interfaces import HashTableBase


class CuckooHash(HashTableBase):
    """Minimal cuckoo hash map with two tables and simple hash functions."""

    def __init__(self, m: int = 8, max_load_factor: float = 0.5, observer=None) -> None:
        super().__init__(observer=observer)
        self.m = max(2, m)  # size of each table
        self.n = 0          # number of stored items
        self.max_load_factor = max_load_factor
        self.t1: list[Optional[tuple[Any, Any]]] = [None] * self.m
        self.t2: list[Optional[tuple[Any, Any]]] = [None] * self.m

    def __len__(self) -> int:
        return self.n

    def capacity(self) -> int:
        return 2 * self.m

    def h1(self, key: Any) -> int:
        self.observer.event("hash_calls_total")
        self.observer.event("hash_calls_h1")
        return hash(key) % self.m

    def h2(self, key: Any) -> int:
        self.observer.event("hash_calls_total")
        self.observer.event("hash_calls_h2")
        return hash(("x", key)) % self.m

    def _probe(self, table: list[Optional[tuple[Any, Any]]], idx: int) -> Optional[tuple[Any, Any]]:
        self.observer.event("memory_probe")
        return table[idx]

    def _write(self, table: list[Optional[tuple[Any, Any]]], idx: int, value: Optional[tuple[Any, Any]]) -> None:
        self.observer.event("memory_write")
        table[idx] = value

    def contains(self, key: Any) -> bool:
        i = self.h1(key)
        x = self._probe(self.t1, i)
        if x is not None and x[0] == key:
            return True

        j = self.h2(key)
        y = self._probe(self.t2, j)
        if y is not None and y[0] == key:
            return True

        return False

    def find(self, key: Any) -> Optional[Any]:
        i = self.h1(key)
        x = self._probe(self.t1, i)
        if x is not None and x[0] == key:
            return x[1]

        j = self.h2(key)
        y = self._probe(self.t2, j)
        if y is not None and y[0] == key:
            return y[1]

        return None

    def delete(self, key: Any) -> bool:
        i = self.h1(key)
        x = self._probe(self.t1, i)
        if x is not None and x[0] == key:
            self._write(self.t1, i, None)
            self.n -= 1
            self.observer.event("delete_success")
            return True

        j = self.h2(key)
        y = self._probe(self.t2, j)
        if y is not None and y[0] == key:
            self._write(self.t2, j, None)
            self.n -= 1
            self.observer.event("delete_success")
            return True

        self.observer.event("delete_miss")
        return False

    def insert(self, key: Any, value: Any) -> bool:
        if self.contains(key):
            self.observer.event("duplicate_insert")
            return False

        if len(self) + 1 > self.max_load_factor * self.capacity():
            self.resize(2 * self.m)

        ok = self._insert_item((key, value))
        if ok:
            self.observer.event("insert_success")
        return ok

    def _insert_item(self, item: tuple[Any, Any]) -> bool:
        cur = item
        table_num = 1
        max_steps = 2 * self.m

        for _ in range(max_steps):
            if table_num == 1:
                idx = self.h1(cur[0])
                x = self._probe(self.t1, idx)
                if x is None:
                    self._write(self.t1, idx, cur)
                    self.n += 1
                    return True
                self.observer.event("collision")
                self.observer.event("relocation")
                self._write(self.t1, idx, cur)
                cur = x
                table_num = 2
            else:
                idx = self.h2(cur[0])
                y = self._probe(self.t2, idx)
                if y is None:
                    self._write(self.t2, idx, cur)
                    self.n += 1
                    return True
                self.observer.event("collision")
                self.observer.event("relocation")
                self._write(self.t2, idx, cur)
                cur = y
                table_num = 1

        self.observer.event("cycle_detected")
        spilled = cur
        self.resize(2 * self.m)
        return self._insert_item(spilled)

    def resize(self, new_m: int) -> None:
        self.observer.event("resize")
        old_items: list[tuple[Any, Any]] = []

        for item in self.t1:
            if item is not None:
                old_items.append(item)
        for item in self.t2:
            if item is not None:
                old_items.append(item)

        self.m = max(2, new_m)
        self.t1 = [None] * self.m
        self.t2 = [None] * self.m
        self.n = 0

        for key, value in old_items:
            self._insert_item((key, value))
