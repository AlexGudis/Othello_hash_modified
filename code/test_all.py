import csv
import json
import random
import sys
from datetime import datetime
from pathlib import Path
from time import perf_counter
from collections import deque
from collections.abc import Mapping

import matplotlib.pyplot as plt

from common import generate_kv
from cuckoo import CuckooHash
from pog_mod import PogControl


# ============================================================
# 1. Генераторы нагрузок
# ============================================================

def make_find_workload(existing_keys, size: int):
    return [("find", random.choice(existing_keys), None) for _ in range(size)]


def make_insert_workload(size: int):
    return [("insert", *generate_kv()) for _ in range(size)]


# ============================================================
# 2. Приблизительная оценка памяти Python-объекта
# ============================================================

def deep_getsizeof(obj, seen=None):
    if seen is None:
        seen = set()

    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)

    size = sys.getsizeof(obj)

    if isinstance(obj, Mapping):
        for k, v in obj.items():
            size += deep_getsizeof(k, seen)
            size += deep_getsizeof(v, seen)
        return size

    if hasattr(obj, "__dict__"):
        size += deep_getsizeof(vars(obj), seen)

    if hasattr(obj, "__slots__"):
        for slot in obj.__slots__:
            if hasattr(obj, slot):
                size += deep_getsizeof(getattr(obj, slot), seen)

    if isinstance(obj, (list, tuple, set, frozenset, deque)):
        for item in obj:
            size += deep_getsizeof(item, seen)
        return size

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

    start = perf_counter()
    if algorithm_factory is PogControl:
        algo = algorithm_factory(default_table)
    else:
        algo = algorithm_factory(n)
        for k, v in default_table.items():
            algo.insert(k, v)
    end = perf_counter()

    return algo, default_table, end - start


def get_measured_object(algo, measure_query_only=False):
    if measure_query_only and isinstance(algo, PogControl):
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
    runner = BenchmarkRunner()

    x_sizes = []
    y_find_time = []
    y_memory_bytes = []
    y_structure_construction_time = []
    y_hash_calls_construction = []
    y_hash_calls_find = []

    for n in sizes:
        total_find_time = 0.0
        total_memory = 0
        total_construct_time = 0.0
        total_hash_calls_construct = 0
        total_hash_calls_find = 0

        for _ in range(avg_factor):
            algo, default_table, build_time = preload_structures(n, algorithm_factory)
            total_hash_calls_construct += algo.metrics_snapshot()["hash_calls_total"]
            
            # Сброс метрик для следующих тестов
            algo.metrics.reset()

            # Время построения структуры
            total_construct_time += build_time

            # Время заданного числа операций поиска (Ключ присутствует)
            find_ops = make_find_workload(list(default_table.keys()), find_ops_count)

            find_results = runner.run(algo, find_ops)
            total_find_time += find_results["elapsed_sec"]
            total_hash_calls_find += find_results["hash_calls_total"]

            # Объём занимаемой памяти
            measured_obj = get_measured_object(algo, measure_query_only=measure_query_only)
            total_memory += deep_getsizeof(measured_obj)

        avg_find_time = total_find_time / avg_factor
        avg_memory = total_memory / avg_factor
        avg_structure_construction_time = total_construct_time / avg_factor
        avg_hash_calls_construct = total_hash_calls_construct / avg_factor
        avg_hash_calls_find = total_hash_calls_find / avg_factor

        x_sizes.append(n)
        y_find_time.append(avg_find_time)
        y_memory_bytes.append(avg_memory)
        y_structure_construction_time.append(avg_structure_construction_time)
        y_hash_calls_construction.append(avg_hash_calls_construct)
        y_hash_calls_find.append(avg_hash_calls_find)

        print(
            f"N={n:6d} | "
            f"find_time={avg_find_time:.6f} sec | "
            f"memory={avg_memory / 1024:.2f} KiB | "
            f"build_time={avg_structure_construction_time:.6f} sec"
        )

    return {
        "sizes": x_sizes,
        "find_time_sec": y_find_time,
        "memory_bytes": y_memory_bytes,
        "construction_time": y_structure_construction_time,
        "hash_call_construction": y_hash_calls_construction,
        "hash_calls_find": y_hash_calls_find,
        # Проверка корректности поиска
    }


# ============================================================
# 6. Сохранение результатов
# ============================================================

def create_output_dir(base_dir="benchmark_results"):
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(base_dir) / run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def save_json(data, path: Path):
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def save_combined_csv(results_cuckoo, results_othello, path: Path):
    sizes = results_cuckoo["sizes"]

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "size",
            "cuckoo_find_time_sec",
            "pog_find_time_sec",
            "cuckoo_memory_bytes",
            "pog_memory_bytes",
            "cuckoo_construction_time_sec",
            "pog_construction_time_sec",
        ])

        for i, size in enumerate(sizes):
            writer.writerow([
                size,
                results_cuckoo["find_time_sec"][i],
                results_othello["find_time_sec"][i],
                results_cuckoo["memory_bytes"][i],
                results_othello["memory_bytes"][i],
                results_cuckoo["construction_time"][i],
                results_othello["construction_time"][i],
            ])


def save_run_metadata(output_dir: Path, sizes, avg_factor, find_ops_count, plot_configs):
    metadata = {
        "sizes": sizes,
        "avg_factor": avg_factor,
        "find_ops_count": find_ops_count,
        "plot_configs": plot_configs,
        "output_dir": str(output_dir),
    }
    save_json(metadata, output_dir / "run_config.json")


# ============================================================
# 7. Универсальные настройки графиков
# ============================================================

def apply_axis_scales(ax, *, x_log=False, y_log=False):
    if x_log:
        ax.set_xscale("log")
    if y_log:
        ax.set_yscale("log")


def make_scale_suffix(*, x_log=False, y_log=False):
    if x_log and y_log:
        return "logx_logy"
    if x_log:
        return "logx"
    if y_log:
        return "logy"
    return "linear"


def setup_plot(ax, xlabel, ylabel, title, *, x_log=False, y_log=False):
    apply_axis_scales(ax, x_log=x_log, y_log=y_log)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()


def plot_metric(
    sizes,
    series1,
    series2,
    *,
    label1,
    label2,
    xlabel,
    ylabel,
    title,
    output_path,
    x_log=False,
    y_log=False,
):
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(sizes, series1, marker="o", label=label1)
    ax.plot(sizes, series2, marker="o", label=label2)

    setup_plot(
        ax,
        xlabel=xlabel,
        ylabel=ylabel,
        title=title,
        x_log=x_log,
        y_log=y_log,
    )

    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


# ============================================================
# 8. Построение конкретных графиков
# ============================================================

def build_all_plots(results_cuckoo, results_othello, output_dir: Path, plot_configs, find_ops_count):
    sizes = results_cuckoo["sizes"]

    find_cfg = plot_configs["find_time"]
    mem_cfg = plot_configs["memory"]
    build_cfg = plot_configs["construction_time"]

    find_suffix = make_scale_suffix(x_log=find_cfg["x_log"], y_log=find_cfg["y_log"])
    mem_suffix = make_scale_suffix(x_log=mem_cfg["x_log"], y_log=mem_cfg["y_log"])
    build_suffix = make_scale_suffix(x_log=build_cfg["x_log"], y_log=build_cfg["y_log"])

    plot_metric(
        sizes,
        results_cuckoo["find_time_sec"],
        results_othello["find_time_sec"],
        label1="CuckooHash",
        label2="Pog/Othello",
        xlabel="Размер множества ключей",
        ylabel="Время серии find-операций, сек",
        title=f"Сравнение времени {find_ops_count} операций поиска",
        output_path=output_dir / f"find_time_{find_suffix}.png",
        x_log=find_cfg["x_log"],
        y_log=find_cfg["y_log"],
    )

    plot_metric(
        sizes,
        [x / 1024 for x in results_cuckoo["memory_bytes"]],
        [x / 1024 for x in results_othello["memory_bytes"]],
        label1="CuckooHash",
        label2="Pog/Othello",
        xlabel="Размер множества ключей",
        ylabel="Память, KiB",
        title="Сравнение занимаемой памяти",
        output_path=output_dir / f"memory_{mem_suffix}.png",
        x_log=mem_cfg["x_log"],
        y_log=mem_cfg["y_log"],
    )

    plot_metric(
        sizes,
        results_cuckoo["construction_time"],
        results_othello["construction_time"],
        label1="CuckooHash",
        label2="Pog/Othello",
        xlabel="Размер множества ключей",
        ylabel="Время построения структуры, сек",
        title="Сравнение времени построения структуры",
        output_path=output_dir / f"construction_time_{build_suffix}.png",
        x_log=build_cfg["x_log"],
        y_log=build_cfg["y_log"],
    )


    plot_metric(
        sizes,
        results_cuckoo["hash_call_construction"],
        results_othello["hash_call_construction"],
        label1="CuckooHash",
        label2="Pog/Othello",
        xlabel="Размер множества ключей",
        ylabel="Число вызовов хеш-функций",
        title="Сравнение числа вызовов хеш-функций во время построения",
        output_path=output_dir / f"hash_calls_construct_{build_suffix}.png",
        x_log=build_cfg["x_log"],
        y_log=build_cfg["y_log"],
    )

    plot_metric(
        sizes,
        results_cuckoo["hash_calls_find"],
        results_othello["hash_calls_find"],
        label1="CuckooHash",
        label2="Pog/Othello",
        xlabel="Размер множества ключей",
        ylabel="Число вызовов хеш-функций",
        title=f"Сравнение числа вызовов хеш-функций во время {find_ops_count} операций поиска",
        output_path=output_dir / f"hash_calls_find_{build_suffix}.png",
        x_log=build_cfg["x_log"],
        y_log=build_cfg["y_log"],
    )


# ============================================================
# 9. Точка входа
# ============================================================

if __name__ == "__main__":
    random.seed(42)

    # sizes = [1000, 2000, 4000, 10000, 20000, 50000, 100000, 200000]
    sizes = [1000, 2000, 4000, 10000]
    avg_factor = 3
    find_ops_count = 50_000

    # Гибкая настройка масштабов для каждого графика
    plot_configs = {
        "find_time": {
            "x_log": False,
            "y_log": False,
        },
        "memory": {
            "x_log": False,
            "y_log": True,
        },
        "construction_time": {
            "x_log": False,
            "y_log": False,
        },
        "hash_call_construction": {
            "x_log": False,
            "y_log": False,
        },
        "hash_calls_find": {
            "x_log": False,
            "y_log": False,
        },
    }

    output_dir = create_output_dir()

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

    save_json(results_cuckoo, output_dir / "results_cuckoo.json")
    save_json(results_othello, output_dir / "results_pog_othello.json")
    save_combined_csv(results_cuckoo, results_othello, output_dir / "combined_results.csv")
    save_run_metadata(output_dir, sizes, avg_factor, find_ops_count, plot_configs)

    build_all_plots(
        results_cuckoo,
        results_othello,
        output_dir=output_dir,
        plot_configs=plot_configs,
        find_ops_count=find_ops_count,
    )

    print(f"\nРезультаты сохранены в папку: {output_dir}")