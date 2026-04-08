# ============================================================
# 1. Метрики
# ============================================================

class HashMetrics:
    def __init__(self):
        self.counters = {
            "hash_calls_total": 0,
            "memory_count": 0,
        }

    def inc(self, name: str, value: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + value

    def snapshot(self) -> dict:
        return dict(self.counters)

    def reset(self) -> None:
        self.counters = {
            "hash_calls_total": 0,
            "memory_count": 0,
        }


# ============================================================
# 2. Базовый класс для алгоритмов
# ============================================================

class HashAlgorithmBase:
    """
    Минимальный базовый класс.

    Чтобы встроить свой алгоритм:
    1) унаследуйся от HashAlgorithmBase
    2) реализуй insert/find/delete
    3) все вызовы хеш-функций делай через self.call_hash(...)
    """

    def __init__(self):
        self.metrics = HashMetrics()
    
    def reset_metrics(self):
        self.metrics.reset()

    def metrics_snapshot(self):
        return self.metrics.snapshot()

    def insert(self, key, value):
        raise NotImplementedError

    def find(self, key):
        raise NotImplementedError

    def delete(self, key):
        raise NotImplementedError