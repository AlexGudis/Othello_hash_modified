import random
import sys
import matplotlib.pyplot as plt
from time import perf_counter
from collections import defaultdict
from collections.abc import Mapping

from common import generate_kv
from cuckoo import CuckooHash
from pog_mod import PogControl
from collections import deque


# ============================================================
# 1. Генераторы нагрузок
# ============================================================

def make_find_workload(existing_keys, size: int):
    """Сгенерировать workload из операций поиска по существующим ключам."""
    return [("find", random.choice(existing_keys), None) for _ in range(size)]


def make_insert_workload(size: int):
    return [("insert", *generate_kv()) for _ in range(size)]


# ============================================================
# 2. Приблизительная оценка памяти Python-объекта
# ============================================================

def deep_getsizeof(obj, seen=None):
    """Рекурсивно оценить объём памяти объекта в байтах.

    Это не идеальный profiler памяти, но для сравнительного теста подходит.
    """
    if seen is None:
        seen = set()

    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)

    size = sys.getsizeof(obj)

    # dict / Mapping
    if isinstance(obj, Mapping):
        for k, v in obj.items():
            size += deep_getsizeof(k, seen)
            size += deep_getsizeof(v, seen)
        return size

    # __dict__
    if hasattr(obj, "__dict__"):
        size += deep_getsizeof(vars(obj), seen)

    # __slots__
    if hasattr(obj, "__slots__"):
        for slot in obj.__slots__:
            if hasattr(obj, slot):
                size += deep_getsizeof(getattr(obj, slot), seen)

    # контейнеры
    if isinstance(obj, (list, tuple, set, frozenset, deque)):
        for item in obj:
            size += deep_getsizeof(item, seen)
        return size

    # Для bitarray, str, bytes, int и т.п. sys.getsizeof обычно уже хватает
    return size


# ============================================================
# 3. Раннер
# ============================================================

class BenchmarkRunner:
    def run(self, algorithm, ops):
        algorithm.reset_metrics()

        start = perf_counter()

        for op, key, value in ops:
            if op == "insert":
                algorithm.insert(key, value)

            elif op == "find":
                algorithm.find(key)

            elif op == "delete":
                algorithm.delete(key)

            else:
                raise ValueError(f"Unknown operation: {op}")

        elapsed = perf_counter() - start
        metrics = algorithm.metrics_snapshot()
        metrics["elapsed_sec"] = elapsed
        return metrics


# ============================================================
# 4. Инициализация структур
# ============================================================

def preload_structures(n, algorithm_factory):
    default_table = {}

    while len(default_table) < n:
        key, value = generate_kv()
        default_table[key] = value

    # Список ключей нужен для workload поиска
    keys = list(default_table.keys())

    start = perf_counter()
    if algorithm_factory is PogControl:
        algo = algorithm_factory(default_table)
    else:
        algo = algorithm_factory(n)
        for k, v in default_table.items():
            algo.insert(k, v)
    end = perf_counter()

    return algo, keys, end - start


def get_measured_object(algo, measure_query_only=False):
    """Что именно мерить по памяти.

    measure_query_only=False:
        меряем весь объект алгоритма.

    measure_query_only=True:
        для PogControl меряем только query structure.
    """
    if measure_query_only and isinstance(algo, PogControl):
        # если у тебя есть публичный метод query(), лучше использовать его
        # return algo.query()
        return algo._query

    return algo


# ============================================================
# 5. Эксперимент
# ============================================================

def experiment(
    algorithm_factory,
    sizes,
    avg_factor,
    find_ops_count,
    *,
    measure_query_only=False,
):
    """
    algorithm_factory:
        класс алгоритма (CuckooHash или PogControl)
    sizes:
        размеры исходного множества
    avg_factor:
        число повторов для усреднения
    find_ops_count:
        сколько операций поиска выполнять на каждом прогоне
    measure_query_only:
        если True, для PogControl память меряется только у query structure
    """
    runner = BenchmarkRunner()

    x_sizes = []
    y_find_time = [] # Время заданного числа операций поиска
    y_memory_bytes = [] # Объём занимаемом структурой памяти
    y_structure_construction_time = [] # Время построения структуры

    for n in sizes:
        total_find_time = 0.0
        total_memory = 0
        total_construct_time = 0.0

        for _ in range(avg_factor):
            algo, keys, time = preload_structures(n, algorithm_factory)
            total_construct_time += time

            # workload только из существующих ключей
            find_ops = make_find_workload(keys, find_ops_count)

            find_results = runner.run(algo, find_ops)
            total_find_time += find_results["elapsed_sec"]

            measured_obj = get_measured_object(algo, measure_query_only=measure_query_only)
            total_memory += deep_getsizeof(measured_obj)

        avg_find_time = total_find_time / avg_factor
        avg_memory = total_memory / avg_factor
        avg_structure_construction_time = total_construct_time / avg_factor

        x_sizes.append(n)
        y_find_time.append(avg_find_time)
        y_memory_bytes.append(avg_memory)
        y_structure_construction_time.append(avg_structure_construction_time)

        print(
            f"N={n:5d} | "
            f"find_time={avg_find_time:.6f} sec | "
            f"memory={avg_memory / 1024:.2f} KiB"
        )

    return {
        "sizes": x_sizes,
        "find_time_sec": y_find_time,
        "memory_bytes": y_memory_bytes,
        "construction_time": y_structure_construction_time
    }


# ============================================================
# 6. Графики
# ============================================================

def plot_find_time(results_cuckoo, results_othello, title):
    sizes = results_cuckoo["sizes"]

    plt.figure(figsize=(9, 5))
    plt.plot(sizes, results_cuckoo["find_time_sec"], marker="o", label="CuckooHash")
    plt.plot(sizes, results_othello["find_time_sec"], marker="o", label="Pog/Othello")

    plt.xlabel("Размер множества ключей")
    plt.ylabel("Время выполнения серии find-операций, сек")
    plt.title(title)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_memory(results_cuckoo, results_othello, title):
    sizes = results_cuckoo["sizes"]

    cuckoo_mem_kib = [x / 1024 for x in results_cuckoo["memory_bytes"]]
    othello_mem_kib = [x / 1024 for x in results_othello["memory_bytes"]]

    plt.figure(figsize=(9, 5))
    plt.plot(sizes, cuckoo_mem_kib, marker="o", label="CuckooHash")
    plt.plot(sizes, othello_mem_kib, marker="o", label="Pog/Othello")

    plt.xlabel("Размер множества ключей")
    plt.ylabel("Память, KiB")
    plt.title(title)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_construction_time(results_cuckoo, results_othello, title):
    sizes = results_cuckoo["sizes"]

    plt.figure(figsize=(9, 5))
    plt.plot(sizes, results_cuckoo["construction_time"], marker="o", label="CuckooHash")
    plt.plot(sizes, results_othello["construction_time"], marker="o", label="Pog/Othello")

    plt.xlabel("Размер множества ключей")
    plt.ylabel("Время построения структуры, сек")
    plt.title(title)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.show()


# ============================================================
# 7. Точка входа
# ============================================================

if __name__ == "__main__":
    random.seed(42)

    sizes = [10000, 20000, 50000, 100000]
    avg_factor = 3
    find_ops_count = 50_000

    # Полная память структур
    results_cuckoo = experiment(
        algorithm_factory=CuckooHash,
        sizes=sizes,
        avg_factor=avg_factor,
        find_ops_count=find_ops_count,
        measure_query_only=False,
    )

    results_othello = experiment(
        algorithm_factory=PogControl,
        sizes=sizes,
        avg_factor=avg_factor,
        find_ops_count=find_ops_count,
        measure_query_only=True,
    )

    plot_find_time(
        results_cuckoo,
        results_othello,
        title=f"Сравнение времени {find_ops_count} операций поиска",
    )

    plot_memory(
        results_cuckoo,
        results_othello,
        title="Сравнение занимаемой памяти",
    )

    plot_construction_time(
        results_cuckoo,
        results_othello,
        title="Сравнение времени построения структуры"
    )

    