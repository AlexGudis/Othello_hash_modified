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
from linear_search import LinearSearchTable
import numpy as np


# ============================================================
# 1. Генераторы нагрузок
# ============================================================

def make_find_workload(existing_keys, size: int):
    return [("find", random.choice(existing_keys), None) for _ in range(size)]


# def make_delete_workload(existing_keys, size: int):
#     return [("delete", random.choice(existing_keys), None) for _ in range(size)]

def make_delete_workload(existing_keys, size: int):
    size = min(size, len(existing_keys))
    keys = random.sample(existing_keys, size)
    return [("delete", key, None) for key in keys]


def make_insert_workload(existing_table, size: int):
    used_keys = set(existing_table.keys())
    ops = []

    while len(ops) < size:
        key, value = generate_kv()
        if key in used_keys:
            continue
        used_keys.add(key)
        ops.append(("insert", key, value))

    return ops


# def make_insert_workload(size: int):
#     return [("insert", *generate_kv()) for _ in range(size)]


def make_real_workload(
    initial_table,
    *,
    duration_sec: int,
    find_rate: int = 250_000,
    upsert_rate: int = 5_000,
    insert_rate: int = 1,
):
    """
    Генерирует mixed workload, близкий к реальному:
    - много find
    - много upsert (delete + insert того же ключа с новым значением)
    - редкие insert новых уникальных ключей

    Возвращает:
    - ops: список операций в формате (op, key, value)
    - meta: сводка по workload
    """
    current_table = dict(initial_table)
    current_keys = list(current_table.keys())

    find_count = int(find_rate * duration_sec)
    upsert_count = int(upsert_rate * duration_sec)
    insert_count = int(insert_rate * duration_sec)

    op_kinds = (
        ["find"] * find_count
        + ["upsert"] * upsert_count
        + ["insert_new"] * insert_count
    )
    random.shuffle(op_kinds)

    ops = []

    for kind in op_kinds:
        if kind == "find":
            key = random.choice(current_keys)
            ops.append(("find", key, None))

        elif kind == "upsert":
            key = random.choice(current_keys)
            old_value = current_table[key]

            new_value = old_value
            while new_value == old_value:
                _, new_value = generate_kv()

            current_table[key] = new_value
            ops.append(("upsert", key, new_value))

        elif kind == "insert_new":
            while True:
                key, value = generate_kv()
                if key not in current_table:
                    break

            current_table[key] = value
            current_keys.append(key)
            ops.append(("insert", key, value))

    meta = {
        "duration_sec": duration_sec,
        "find_rate": find_rate,
        "upsert_rate": upsert_rate,
        "insert_rate": insert_rate,
        "find_count": find_count,
        "upsert_count": upsert_count,
        "insert_count": insert_count,
        "logical_ops_total": len(ops),
        "primitive_ops_total": find_count + 2 * upsert_count + insert_count,
        "target_logical_ops_per_sec": find_rate + upsert_rate + insert_rate,
        "target_primitive_ops_per_sec": find_rate + 2 * upsert_rate + insert_rate,
    }

    return ops, meta


def workload_file_path(dataset_dir: Path, n: int, avg_idx: int, profile_name: str) -> Path:
    return dataset_dir / f"workload_{profile_name}_n{n}_avg{avg_idx}.json"


def get_or_create_real_workload(
    dataset_dir: Path,
    initial_table,
    n: int,
    avg_idx: int,
    *,
    profile_name: str,
    duration_sec: int,
    find_rate: int,
    upsert_rate: int,
    insert_rate: int,
):
    path = workload_file_path(dataset_dir, n, avg_idx, profile_name)

    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        print(f"Загружен workload из файла: {path}")
        return payload["ops"], payload["meta"]

    ops, meta = make_real_workload(
        initial_table,
        duration_sec=duration_sec,
        find_rate=find_rate,
        upsert_rate=upsert_rate,
        insert_rate=insert_rate,
    )

    payload = {
        "ops": ops,
        "meta": meta,
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Сгенерирован workload: {path}")
    return ops, meta

        



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
            elif op == "upsert":
                algorithm.delete(key)
                algorithm.insert(key, value)
            else:
                raise ValueError(f"Unknown operation: {op}")

        elapsed = perf_counter() - start
        metrics = algorithm.metrics_snapshot()
        metrics["elapsed_sec"] = elapsed
        return metrics


# ============================================================
# 4. Инициализация структур
# ============================================================
def generate_table_json(path: Path, num_entries: int):
    """Генерирует JSON-файл с num_entries уникальными парами MAC-VLAN."""
    data = {}

    while len(data) < num_entries:
        key, value = generate_kv()
        data[key] = value

    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Сгенерирован файл таблицы: {path}")
    return data


def dataset_file_path(dataset_dir: Path, n: int, avg_idx: int) -> Path:
    return dataset_dir / f"table_n{n}_avg{avg_idx}.json"


def load_table_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)
    

def get_or_create_table(dataset_dir: Path, n: int, avg_idx: int):
    path = dataset_file_path(dataset_dir, n, avg_idx)

    if path.exists():
        table = load_table_json(path)
        print(f"Загружена таблица из файла: {path}")
        return table

    return generate_table_json(path, n)



def construct_structures(default_table, algorithm_factory):
    start = perf_counter()

    if algorithm_factory is PogControl:
        algo = algorithm_factory(default_table)
    else:
        algo = algorithm_factory(len(default_table))
        for k, v in default_table.items():
            algo.insert(k, v)

    end = perf_counter()
    return algo, end - start


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
    delete_coeff,
    insert_coeff,
    *,
    measure_query_only=False,
):
    runner = BenchmarkRunner()

    x_sizes = []
    y_memory_bytes = []

    y_structure_construction_time = []
    y_hash_calls_construction = []
    y_memory_counts_construction = []

    y_find_time = []
    y_hash_calls_find = []
    y_memory_counts_find = []

    y_hash_calls_delete = []
    y_memory_counts_delete = []

    y_insert_time = []
    y_hash_calls_insert = []
    y_memory_counts_insert = []
    y_reconstruction_count_insert = []
    



    for n in sizes:
        total_memory = 0

        total_construct_time = 0.0
        total_hash_calls_construct = 0
        total_memory_counts_construction = 0
        
        total_find_time = 0.0
        total_hash_calls_find = 0
        total_memory_counts_find = 0

        total_hash_calls_delete = 0
        total_memory_counts_delete = 0

        total_insert_time = 0
        total_hash_calls_insert = 0
        total_memory_counts_insert = 0
        total_reconstruction_count_insert = 0

        for avg_idx in range(avg_factor):
            default_table = get_or_create_table(
                dataset_dir,
                n,
                avg_idx,
            )

            # Тестирование построения структуры
            algo_build, build_time = construct_structures(default_table, algorithm_factory)
            total_hash_calls_construct += algo_build.metrics_snapshot()["hash_calls_total"]
            total_memory_counts_construction += algo_build.metrics_snapshot()["memory_count"]
            # Время построения структуры
            total_construct_time += build_time

            # Объём занимаемой памяти
            measured_obj = get_measured_object(algo_build, measure_query_only=measure_query_only)
            total_memory += deep_getsizeof(measured_obj)
            


            # Тестирование операции find
            # Время заданного числа операций поиска (Ключ присутствует)
            # TODO: корректность операций поиска (Важно для Отелло)
            algo_find, _ = construct_structures(default_table, algorithm_factory)
            find_ops = make_find_workload(list(default_table.keys()), find_ops_count)
            find_results = runner.run(algo_find, find_ops)
            total_find_time += find_results["elapsed_sec"]
            total_hash_calls_find += find_results["hash_calls_total"]
            total_memory_counts_find += find_results["memory_count"]


            # Тестирование операции delete
            algo_delete, _ = construct_structures(default_table, algorithm_factory)
            delete_ops = make_delete_workload(list(default_table.keys()), int(n * delete_coeff))
            delete_results = runner.run(algo_delete, delete_ops)
            total_hash_calls_delete += delete_results["hash_calls_total"]
            total_memory_counts_delete += delete_results["memory_count"]


            # Тестирование операции вставка
            algo_insert, _ = construct_structures(default_table, algorithm_factory)
            insert_ops = make_insert_workload(default_table, int(n * insert_coeff))
            insert_results = runner.run(algo_insert, insert_ops)
            total_insert_time += insert_results["elapsed_sec"]
            total_hash_calls_insert += insert_results["hash_calls_total"]
            total_memory_counts_insert += insert_results["memory_count"]
            total_reconstruction_count_insert += insert_results["reconstruction_count"]



        avg_memory = total_memory / avg_factor

        avg_structure_construction_time = total_construct_time / avg_factor
        avg_hash_calls_construct = total_hash_calls_construct / avg_factor
        avg_memory_counts_construction = total_memory_counts_construction / avg_factor

        # Нас интересует число вызовов хеш-функций на одну операцию, а не на какое-то их количество
        avg_find_time = total_find_time / avg_factor
        avg_hash_calls_find = total_hash_calls_find / avg_factor / find_ops_count
        avg_memory_counts_find = total_memory_counts_find / avg_factor / find_ops_count

        avg_hash_calls_delete = total_hash_calls_delete / avg_factor / int(n * delete_coeff)
        avg_memory_counts_delete = total_memory_counts_delete / avg_factor / int(n * delete_coeff)

        avg_insert_time = total_insert_time / avg_factor
        avg_hash_calls_insert = total_hash_calls_insert / avg_factor / int(n * insert_coeff)
        avg_memory_counts_insert = total_memory_counts_insert / avg_factor / int(n * insert_coeff)
        avg_reconstruction_count_insert = total_reconstruction_count_insert / avg_factor


        x_sizes.append(n)
        y_memory_bytes.append(avg_memory)

        y_structure_construction_time.append(avg_structure_construction_time)
        y_hash_calls_construction.append(avg_hash_calls_construct)
        y_memory_counts_construction.append(avg_memory_counts_construction)

        y_find_time.append(avg_find_time)
        y_hash_calls_find.append(avg_hash_calls_find)
        y_memory_counts_find.append(avg_memory_counts_find)

        y_hash_calls_delete.append(avg_hash_calls_delete)
        y_memory_counts_delete.append(avg_memory_counts_delete)

        y_insert_time.append(avg_insert_time)
        y_hash_calls_insert.append(avg_hash_calls_insert)
        y_memory_counts_insert.append(avg_memory_counts_insert)
        y_reconstruction_count_insert.append(avg_reconstruction_count_insert)

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
        "memory_count_construction": y_memory_counts_construction,
        "hash_calls_find": y_hash_calls_find,
        "memory_count_find": y_memory_counts_find,
        "hash_calls_delete": y_hash_calls_delete,
        "memory_count_delete": y_memory_counts_delete,
        "insert_time_sec": y_insert_time,
        "hash_calls_insert": y_hash_calls_insert,
        "memory_count_insert": y_memory_counts_insert,
        "reconstruction_count_insert": y_reconstruction_count_insert,
        # Проверка корректности поиска
    }




def experiment_realistic(
    algorithm_factory,
    sizes,
    avg_factor,
    *,
    dataset_dir: Path,
    duration_sec,
    find_rate,
    upsert_rate,
    insert_rate,
    measure_query_only=False,
):
    runner = BenchmarkRunner()

    x_sizes = []
    y_build_time = []
    y_mixed_time = []
    y_keep_up_ratio = []
    y_memory_bytes = []
    y_hash_calls_total = []
    y_memory_count_total = []

    profile_name = f"d{duration_sec}_f{find_rate}_u{upsert_rate}_i{insert_rate}"

    for n in sizes:
        total_build_time = 0.0
        total_elapsed = 0.0
        total_memory = 0
        total_keep_up_ratio = 0.0
        total_hash_calls_total = 0.0
        total_memory_count_total = 0.0

        for avg_idx in range(avg_factor):
            default_table = get_or_create_table(dataset_dir, n, avg_idx)

            ops, meta = get_or_create_real_workload(
                dataset_dir=dataset_dir,
                initial_table=default_table,
                n=n,
                avg_idx=avg_idx,
                profile_name=profile_name,
                duration_sec=duration_sec,
                find_rate=find_rate,
                upsert_rate=upsert_rate,
                insert_rate=insert_rate,
            )

            algo, build_time = construct_structures(default_table, algorithm_factory)
            run_results = runner.run(algo, ops)
            elapsed = run_results["elapsed_sec"]

            """
            логических операций в секунду:

            250000 + 5000 + 1 = 255001

            примитивных операций в секунду:

            250000 * 1 + 5000 * 2 + 1 * 1 = 260001

            Потому что каждый upsert внутри стоит как две базовые команды.
            """

            logical_throughput = meta["logical_ops_total"] / elapsed
            keep_up_ratio = logical_throughput / meta["target_logical_ops_per_sec"]

            hash_calls_total = run_results.get("hash_calls_total", 0)
            memory_count_total = run_results.get("memory_count", 0)

            total_build_time += build_time
            total_elapsed += elapsed
            total_keep_up_ratio += keep_up_ratio
            total_hash_calls_total += hash_calls_total
            total_memory_count_total += memory_count_total

            measured_obj = get_measured_object(algo, measure_query_only=measure_query_only)
            total_memory += deep_getsizeof(measured_obj)

        x_sizes.append(n)
        y_build_time.append(total_build_time / avg_factor)
        y_mixed_time.append(total_elapsed / avg_factor)
        y_keep_up_ratio.append(total_keep_up_ratio / avg_factor)
        y_memory_bytes.append(total_memory / avg_factor)
        y_hash_calls_total.append(total_hash_calls_total / avg_factor)
        y_memory_count_total.append(total_memory_count_total / avg_factor)

        print(
            f"N={n:6d} | "
            f"mixed_time={y_mixed_time[-1]:.6f} sec | "
            f"keep_up={y_keep_up_ratio[-1]:.2f}x"
        )

    return {
        "sizes": x_sizes,
        "build_time_sec": y_build_time,
        "mixed_time_sec": y_mixed_time,
        "keep_up_ratio": y_keep_up_ratio,
        "memory_bytes": y_memory_bytes,
        "hash_calls_total": y_hash_calls_total,
        "memory_count_total": y_memory_count_total,
        "profile": {
            "duration_sec": duration_sec,
            "find_rate": find_rate,
            "upsert_rate": upsert_rate,
            "insert_rate": insert_rate,
            "target_logical_ops_per_sec": find_rate + upsert_rate + insert_rate,
            "target_primitive_ops_per_sec": find_rate + 2 * upsert_rate + insert_rate,
        },
    }


# ============================================================
# 6. Сохранение результатов
# ============================================================

def create_output_dir(base_dir="test_results"):
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


def save_run_metadata(output_dir: Path, sizes, avg_factor, find_ops_count):
    metadata = {
        "sizes": sizes,
        "avg_factor": avg_factor,
        "find_ops_count": find_ops_count,
        "output_dir": str(output_dir),
    }
    save_json(metadata, output_dir / "run_config.json")


# ============================================================
# 7. Универсальные настройки графиков
# ============================================================
def apply_tight_ylim(ax, values, *, y_log=False, pad_ratio=0.08):
    vals = [float(v) for v in values if v is not None]

    if not vals:
        return

    vmin = min(vals)
    vmax = max(vals)

    if y_log:
        positive_vals = [v for v in vals if v > 0]
        if not positive_vals:
            return
        vmin = min(positive_vals)
        vmax = max(positive_vals)

        if vmin == vmax:
            ax.set_ylim(vmin / 1.2, vmax * 1.2)
        else:
            ax.set_ylim(vmin / (1 + pad_ratio), vmax * (1 + pad_ratio))
    else:
        if vmin == vmax:
            pad = max(abs(vmin) * pad_ratio, 0.05)
        else:
            pad = (vmax - vmin) * pad_ratio

        ax.set_ylim(vmin - pad, vmax + pad)


def apply_axis_scales(ax, *, x_log=False, y_log=False):
    if x_log:
        ax.set_xscale("log")
    if y_log:
        ax.set_yscale("log")


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
    series3=None,
    *,
    label1,
    label2,
    label3=None,
    xlabel,
    ylabel,
    title,
    output_path,
    x_log=False,
    y_log=False,
    tight_y=False,
    y_pad_ratio=0.08,
    annotate=False,
    kind="line",   # "line" или "heatmap"
    cmap="viridis",
):
    if kind == "heatmap":
        data_list = [series1, series2]
        labels = [label1, label2]

        if series3 is not None:
            data_list.append(series3)
            labels.append(label3)

        data = np.array(data_list, dtype=float)

        fig, ax = plt.subplots(figsize=(max(7, len(sizes) * 1.2), 2.8))
        im = ax.imshow(data, aspect="auto", cmap=cmap)

        ax.set_yticks(range(len(labels)))
        ax.set_yticklabels(labels)

        ax.set_xticks(range(len(sizes)))
        ax.set_xticklabels(sizes)

        ax.set_xlabel(xlabel)
        ax.set_title(title)

        cbar = fig.colorbar(im, ax=ax)
        cbar.set_label(ylabel)

        if annotate:
            mean_val = float(np.mean(data))
            for i in range(data.shape[0]):
                for j in range(data.shape[1]):
                    value = data[i, j]
                    color = "white" if value > mean_val else "black"
                    ax.text(j, i, f"{value:.3g}", ha="center", va="center", color=color, fontsize=9)

        fig.tight_layout()
        fig.savefig(output_path, dpi=200, bbox_inches="tight")
        plt.close(fig)
        return

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(sizes, series1, marker="o", label=label1)
    ax.plot(sizes, series2, marker="o", label=label2)
    
    if series3 is not None:
        ax.plot(sizes, series3, marker="o", label=label3)

    setup_plot(
        ax,
        xlabel=xlabel,
        ylabel=ylabel,
        title=title,
        x_log=x_log,
        y_log=y_log,
    )

    if tight_y:
        apply_tight_ylim(
            ax,
            list(series1) + list(series2),
            y_log=y_log,
            pad_ratio=y_pad_ratio,
        )

    if annotate:
        for x, y in zip(sizes, series1):
            ax.annotate(f"{y:.2f}", (x, y), textcoords="offset points", xytext=(0, 6), ha="center", fontsize=8)

        for x, y in zip(sizes, series2):
            ax.annotate(f"{y:.2f}", (x, y), textcoords="offset points", xytext=(0, -12), ha="center", fontsize=8)
        
        if series3 is not None:
            for x, y in zip(sizes, series3):
                ax.annotate(f"{y:.2f}", (x, y), textcoords="offset points", xytext=(0, -12), ha="center", fontsize=8)


    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)


# ============================================================
# 8. Построение конкретных графиков
# ============================================================

def build_all_plots(results_cuckoo, results_othello, results_linear, output_dir: Path, find_ops_count, insert_coeff, linear_check=False):
    sizes = results_cuckoo["sizes"]

    plots = [
        {
            "key": "find_time_sec",
            "ylabel": "Время серии find-операций, сек",
            "title": f"Время {find_ops_count} операций поиска",
            "filename": "time_find.png",
            "annotate": True,
        },
        {
            "key": "memory_bytes",
            "transform": lambda x: x / 1024,
            "ylabel": "Память, KiB",
            "title": "Объём занимаемой памяти",
            "filename": "memory.png",
            "y_log": True,
            "annotate": True,
        },
        {
            "key": "construction_time",
            "ylabel": "Время построения структуры, сек",
            "title": "Время построения структуры",
            "filename": "time_construction.png",
            "annotate": True,
        },
        {
            "key": "hash_call_construction",
            "ylabel": "Число вызовов хеш-функций",
            "title": "Хеш-функции при построении",
            "filename": "hash_calls_construct.png",
            "annotate": True,
        },
        {
            "key": "memory_count_construction",
            "ylabel": "Число обращений к памяти",
            "title": "Обращения к памяти при построении",
            "filename": "memory_count_construction.png",
            "annotate": True,
        },
        {
            "key": "hash_calls_find",
            "ylabel": "Число вызовов хеш-функций",
            "title": "Хеш-функции при поиске",
            "filename": "hash_calls_find.png",
        },
        {
            "key": "memory_count_find",
            "ylabel": "Число обращений к памяти",
            "title": "Обращения к памяти при поиске",
            "filename": "memory_count_find.png",
        },
        {
            "key": "hash_calls_delete",
            "ylabel": "Число вызовов хеш-функций",
            "title": "Хеш-функции при удалении",
            "filename": "hash_calls_delete.png",
        },
        {
            "key": "memory_count_delete",
            "ylabel": "Число обращений к памяти",
            "title": "Обращения к памяти при удалении",
            "filename": "memory_count_delete.png",
        },
        {
            "key": "insert_time_sec",
            "ylabel": "Время серии вставок, сек",
            "title": f"Время вставки правил",
            "filename": "time_insert.png",
            "annotate": True,
        },
        {
            "key": "hash_calls_insert",
            "ylabel": "Число вызовов хеш-функций",
            "title": f"Хеш-функции при вставке",
            "filename": "hash_calls_insert.png",
            "annotate": True,
        },
        {
            "key": "memory_count_insert",
            "ylabel": "Число обращений к памяти",
            "title": "Обращения к памяти при вставке",
            "filename": "memory_count_insert.png",
            "annotate": True,
        },
        {
            "key": "reconstruction_count_insert",
            "ylabel": "Число перестроений",
            "title": f"Число перестроений при увеличении числа ключей на {int(insert_coeff * 100)}%",
            "filename": "reconstruction_count_insert.png",
        },
    ]

    for cfg in plots:
        key = cfg["key"]

        transform = cfg.get("transform", lambda x: x)

        series1 = [transform(x) for x in results_cuckoo[key]]
        series2 = [transform(x) for x in results_othello[key]]
        series3 = None
        if linear_check:
            series3 = [transform(x) for x in results_linear[key]]

        plot_metric(
            sizes,
            series1,
            series2,
            series3,
            label1="CuckooHash",
            label2="Pog/Othello",
            label3="LinearSearch",
            xlabel="Размер множества ключей",
            ylabel=cfg["ylabel"],
            title=cfg["title"],
            output_path=output_dir / cfg["filename"],
            x_log=cfg.get("x_log", False),
            y_log=cfg.get("y_log", False),
            tight_y=cfg.get("tight_y", False),
            y_pad_ratio=cfg.get("y_pad_ratio", 0.08),
            annotate=cfg.get("annotate", False),
            kind=cfg.get("kind", "line"),
            cmap=cfg.get("cmap", "viridis"),
        )


def build_realistic_plots(results_cuckoo, results_othello, output_dir: Path):
    sizes = results_cuckoo["sizes"]

    plots = [
        ("build_time_sec", "Время построения структуры, сек", "Время построения структуры", "real_build_time.png", False, True),
        ("mixed_time_sec", "Время выполнения смешанной нагрузки, сек", "Время выполнения", "real_mixed_time.png", False, False),
        ("keep_up_ratio", "Запас по производительности", "Отношение числа выполненных операций к целевому в секунду", "real_keep_up_ratio.png", False, False),
        ("memory_bytes", "Память, KiB", "Память итоговой структуры после смешанной нагрузки", "real_memory.png", False, True),
        ("hash_calls_total", "Число вызовов хеш-функций", "Общее число вызовов хеш-функций", "real_hash_calls_total.png", False, False),
        ("memory_count_total", "Число обращений к памяти", "Общее число обращений к памяти", "real_memory_count_total.png", False, False),
    ]

    for metric_key, ylabel, title, filename, x_log, y_log in plots:
        series1 = results_cuckoo[metric_key]
        series2 = results_othello[metric_key]

        if metric_key == "memory_bytes":
            series1 = [x / 1024 for x in series1]
            series2 = [x / 1024 for x in series2]

        plot_metric(
            sizes,
            series1,
            series2,
            label1="CuckooHash",
            label2="Pog/Othello",
            xlabel="Размер множества ключей",
            ylabel=ylabel,
            title=title,
            output_path=output_dir / filename,
            x_log=x_log,
            y_log=y_log,
            annotate=(metric_key not in {"hash_calls_total", "memory_count_total"}),
        )
# ============================================================
# 9. Точка входа
# ============================================================

if __name__ == "__main__":
    random.seed(42)

    sintetic_test = True
    linear_check = False
    insert_coeff=0.1
    if sintetic_test:
        # sizes = [1000, 2000, 4000, 10000, 20000, 50000, 100000, 200000]
        # sizes = [1000, 2000, 4000, 6000, 8000, 10_000, 15_000, 20_000, 30_000, 40_000, 50_000]
        # sizes = [100, 500, 1000]
        sizes = [1000, 2000, 4000, 6000, 8000, 10_000, 15_000, 20_000]
        avg_factor = 5
        find_ops_count = 50_000
        dataset_dir = Path("datasets")
        output_dir = create_output_dir()

        results_cuckoo = experiment(
            algorithm_factory=CuckooHash,
            sizes=sizes,
            avg_factor=avg_factor,
            find_ops_count=find_ops_count,
            delete_coeff=0.1,
            insert_coeff=insert_coeff,
            measure_query_only=False,
        )

        results_othello = experiment(
            algorithm_factory=PogControl,
            sizes=sizes,
            avg_factor=avg_factor,
            find_ops_count=find_ops_count,
            delete_coeff=0.1,
            insert_coeff=insert_coeff,
            measure_query_only=True,
        )

        results_linear = None
        if linear_check:

            results_linear = experiment(
                algorithm_factory=LinearSearchTable,
                sizes=sizes,
                avg_factor=avg_factor,
                find_ops_count=find_ops_count,
                delete_coeff=0.1,
                insert_coeff=insert_coeff,
                measure_query_only=False,
            )
            save_json(results_linear, output_dir / "results_linear_search.json")

        save_json(results_cuckoo, output_dir / "results_cuckoo.json")
        save_json(results_othello, output_dir / "results_pog_othello.json")
        save_combined_csv(results_cuckoo, results_othello, output_dir / "combined_results.csv")
        save_run_metadata(output_dir, sizes, avg_factor, find_ops_count)

        build_all_plots(
            results_cuckoo,
            results_othello,
            results_linear,
            output_dir=output_dir,
            find_ops_count=find_ops_count,
            insert_coeff=insert_coeff,
            linear_check=linear_check
        )

        print(f"\nРезультаты сохранены в папку: {output_dir}")
    else:
        sizes = [30, 40, 50, 60]
        avg_factor = 3
        dataset_dir = Path("datasets")
        output_dir = create_output_dir()

        realistic_profile = {
            "duration_sec": 2,
            "find_rate": 240_000,
            "upsert_rate": 2200,
            "insert_rate": 1,
        }

        results_cuckoo_real = experiment_realistic(
            algorithm_factory=CuckooHash,
            sizes=sizes,
            avg_factor=avg_factor,
            dataset_dir=dataset_dir,
            duration_sec=realistic_profile["duration_sec"],
            find_rate=realistic_profile["find_rate"],
            upsert_rate=realistic_profile["upsert_rate"],
            insert_rate=realistic_profile["insert_rate"],
            measure_query_only=False,
        )

        results_othello_real = experiment_realistic(
            algorithm_factory=PogControl,
            sizes=sizes,
            avg_factor=avg_factor,
            dataset_dir=dataset_dir,
            duration_sec=realistic_profile["duration_sec"],
            find_rate=realistic_profile["find_rate"],
            upsert_rate=realistic_profile["upsert_rate"],
            insert_rate=realistic_profile["insert_rate"],
            measure_query_only=True,
        )

        results_linear_real = experiment_realistic(
            algorithm_factory=LinearSearchTable,
            sizes=sizes,
            avg_factor=avg_factor,
            dataset_dir=dataset_dir,
            duration_sec=realistic_profile["duration_sec"],
            find_rate=realistic_profile["find_rate"],
            upsert_rate=realistic_profile["upsert_rate"],
            insert_rate=realistic_profile["insert_rate"],
            measure_query_only=False,
        )

        save_json(results_cuckoo_real, output_dir / "results_cuckoo_realistic.json")
        save_json(results_othello_real, output_dir / "results_pog_realistic.json")
        save_json(results_linear_real, output_dir / "results_linear_realistic.json")
        save_json(realistic_profile, output_dir / "realistic_profile.json")

        build_realistic_plots(
            results_cuckoo_real,
            results_othello_real,
            output_dir=output_dir,
        )

        print(f"\nРезультаты реального эксперимента сохранены в папку: {output_dir}")