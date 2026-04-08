#!/usr/bin/env python3

from __future__ import annotations

import sys
import subprocess
from pathlib import Path
from collections import Counter
from typing import Iterator, Tuple

import pandas as pd
import matplotlib.pyplot as plt
import json


# Формат события из test1.cpp:
#   ts_us<TAB>key_u64<TAB>dst_port
#
# key_u64 = (dst_mac << 12) | vlan


def log(msg: str) -> None:
    print(f"[plot_results] {msg}", file=sys.stderr)


def add_mean_line(values, *, color="red", linestyle="--", linewidth=1.5, label_prefix="Среднее"):
    mean_value = values.mean()
    plt.axhline(
        mean_value,
        color=color,
        linestyle=linestyle,
        linewidth=linewidth,
        label=f"{label_prefix}: {mean_value:.2f}",
    )


def decode_key_u64(key: int) -> tuple[int, int]:
    vlan = key & 0xFFF
    dst_mac = key >> 12
    return dst_mac, vlan


def mac_to_str(mac: int) -> str:
    return ":".join(f"{(mac >> shift) & 0xFF:02x}" for shift in range(40, -1, -8))


def save_unique_triples_json(triple_first_us: dict[tuple[int, int], int], path: Path) -> None:
    dataset = {}

    for (key, port), first_us in sorted(triple_first_us.items(), key=lambda x: (x[0][0], x[0][1])):
        dst_mac, vlan = decode_key_u64(key)
        dataset[mac_to_str(dst_mac) + '-' + str(vlan)] = port

    with path.open("w", encoding="utf-8") as f:
        json.dump(dataset, f, ensure_ascii=False, indent=2)


def open_event_stream(path: Path) -> Iterator[Tuple[int, int, int]]:
    """
    Yield (ts_us, key, port) from:
      - plain .tsv
      - .tsv.zst
    """
    if path.suffix == ".zst":
        proc = subprocess.Popen(
            ["zstd", "-dc", str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert proc.stdout is not None

        try:
            for raw_line in proc.stdout:
                line = raw_line.rstrip("\n")
                if not line:
                    continue
                parts = line.split("\t")
                if len(parts) != 3:
                    raise RuntimeError(
                        f"Bad event format in {path}: expected 3 TAB-separated fields "
                        f"(ts_us, key_u64, port), got {len(parts)}: {line[:200]!r}"
                    )
                ts_us_str, key_str, port_str = parts
                yield int(ts_us_str), int(key_str), int(port_str)
        finally:
            proc.stdout.close()
            stderr_text = ""
            if proc.stderr is not None:
                stderr_text = proc.stderr.read()
                proc.stderr.close()
            rc = proc.wait()
            if rc != 0:
                raise RuntimeError(f"zstd failed for {path}: {stderr_text.strip()}")
    else:
        with path.open("r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.rstrip("\n")
                if not line:
                    continue
                parts = line.split("\t")
                if len(parts) != 3:
                    raise RuntimeError(
                        f"Bad event format in {path}: expected 3 TAB-separated fields "
                        f"(ts_us, key_u64, port), got {len(parts)}: {line[:200]!r}"
                    )
                ts_us_str, key_str, port_str = parts
                yield int(ts_us_str), int(key_str), int(port_str)


def collect_event_files(results_dir: Path) -> list[Path]:
    files = sorted(results_dir.glob("*.events.tsv")) + sorted(results_dir.glob("*.events.tsv.zst"))
    return sorted(files)


def update_min(d: dict, key, value: int) -> None:
    old = d.get(key)
    if old is None or value < old:
        d[key] = value


def sparse_counter_to_dense_df(counter: Counter, ts_min: int, ts_max: int, step_sec: int, value_name: str) -> pd.DataFrame:
    if ts_min > ts_max:
        return pd.DataFrame(columns=["timestamp", value_name, "datetime"])

    timestamps = list(range(ts_min, ts_max + 1, step_sec))
    values = [counter.get(ts, 0) for ts in timestamps]
    df = pd.DataFrame({
        "timestamp": timestamps,
        value_name: values,
    })
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    return df


def cumulative_from_new_counts(new_counter: Counter, ts_min: int, ts_max: int, step_sec: int, value_name: str) -> pd.DataFrame:
    df = sparse_counter_to_dense_df(new_counter, ts_min, ts_max, step_sec, "new_count")
    if df.empty:
        return pd.DataFrame(columns=["timestamp", value_name, "datetime"])
    df[value_name] = df["new_count"].cumsum()
    return df[["timestamp", value_name, "datetime"]]


def save_tsv(df: pd.DataFrame, path: Path, value_cols: list[str]) -> None:
    cols = ["timestamp"] + value_cols
    df[cols].to_csv(path, sep="\t", header=False, index=False)


def main() -> None:
    # Совместимо с разными вариантами run.sh:
    #   ./plot_results.py
    #   ./plot_results.py out
    #   ./plot_results.py out results_map
    out_dir = Path(sys.argv[1]) if len(sys.argv) >= 2 else Path("out")
    results_dir = Path(sys.argv[2]) if len(sys.argv) >= 3 else Path("results_map")

    plots_dir = out_dir / "plots"
    out_dir.mkdir(parents=True, exist_ok=True)
    plots_dir.mkdir(parents=True, exist_ok=True)

    event_files = collect_event_files(results_dir)
    if not event_files:
        raise RuntimeError(f"No *.events.tsv or *.events.tsv.zst files found in {results_dir}")

    # Счётчики запросов
    requests_per_sec = Counter()
    requests_per_5sec = Counter()

    # first-seen для пар и триплетов
    pair_first_us: dict[int, int] = {}
    triple_first_us: dict[tuple[int, int], int] = {}

    global_min_sec = None
    global_max_sec = None

    total_events = 0

    for path in event_files:
        log(f"reading {path}")
        for ts_us, key, port in open_event_stream(path):
            sec = ts_us // 1_000_000
            sec5 = (sec // 5) * 5

            requests_per_sec[sec] += 1
            requests_per_5sec[sec5] += 1

            update_min(pair_first_us, key, ts_us)
            update_min(triple_first_us, (key, port), ts_us)

            if global_min_sec is None or sec < global_min_sec:
                global_min_sec = sec
            if global_max_sec is None or sec > global_max_sec:
                global_max_sec = sec

            total_events += 1

        break

    if global_min_sec is None or global_max_sec is None:
        raise RuntimeError("No events found in input files")

    log(f"total events: {total_events}")
    log(f"unique pairs: {len(pair_first_us)}")
    log(f"unique triples: {len(triple_first_us)}")

    # New pairs per time bucket = число insert
    new_pairs_per_sec = Counter()
    new_pairs_per_5sec = Counter()

    for first_us in pair_first_us.values():
        sec = first_us // 1_000_000
        sec5 = (sec // 5) * 5
        new_pairs_per_sec[sec] += 1
        new_pairs_per_5sec[sec5] += 1

    # New triples per time bucket -> для cumulative_unique_triples
    new_triples_per_sec = Counter()
    new_triples_per_5sec = Counter()

    for first_us in triple_first_us.values():
        sec = first_us // 1_000_000
        sec5 = (sec // 5) * 5
        new_triples_per_sec[sec] += 1
        new_triples_per_5sec[sec5] += 1

    # Dense tables
    df_requests_sec = sparse_counter_to_dense_df(
        requests_per_sec, global_min_sec, global_max_sec, 1, "requests"
    )
    df_requests_5sec = sparse_counter_to_dense_df(
        requests_per_5sec, (global_min_sec // 5) * 5, (global_max_sec // 5) * 5, 5, "requests"
    )

    df_new_pairs_sec = sparse_counter_to_dense_df(
        new_pairs_per_sec, global_min_sec, global_max_sec, 1, "new_pairs"
    )
    df_new_pairs_5sec = sparse_counter_to_dense_df(
        new_pairs_per_5sec, (global_min_sec // 5) * 5, (global_max_sec // 5) * 5, 5, "new_pairs"
    )

    df_new_triples_sec = sparse_counter_to_dense_df(
        new_triples_per_sec, global_min_sec, global_max_sec, 1, "new_triples"
    )
    df_new_triples_5sec = sparse_counter_to_dense_df(
        new_triples_per_5sec, (global_min_sec // 5) * 5, (global_max_sec // 5) * 5, 5, "new_triples"
    )

    df_cum_pairs = cumulative_from_new_counts(
        new_pairs_per_sec, global_min_sec, global_max_sec, 1, "cumulative_unique_pairs"
    )
    df_cum_triples = cumulative_from_new_counts(
        new_triples_per_sec, global_min_sec, global_max_sec, 1, "cumulative_unique_triples"
    )

    # Save TSV
    save_tsv(df_cum_pairs, out_dir / "cumulative_unique_pairs.tsv", ["cumulative_unique_pairs"])
    save_tsv(df_cum_triples, out_dir / "cumulative_unique_triples.tsv", ["cumulative_unique_triples"])
    save_tsv(df_new_pairs_sec, out_dir / "new_pairs_per_sec.tsv", ["new_pairs"])
    save_tsv(df_new_pairs_5sec, out_dir / "new_pairs_per_5sec.tsv", ["new_pairs"])
    save_tsv(df_new_triples_sec, out_dir / "new_triples_per_sec.tsv", ["new_triples"])
    save_tsv(df_new_triples_5sec, out_dir / "new_triples_per_5sec.tsv", ["new_triples"])
    save_tsv(df_requests_sec, out_dir / "requests_per_sec.tsv", ["requests"])
    save_tsv(df_requests_5sec, out_dir / "requests_per_5sec.tsv", ["requests"])

    # Summary
    summary_path = out_dir / "summary_metrics.tsv"
    with summary_path.open("w", encoding="utf-8") as f:
        f.write(f"total_events\t{total_events}\n")
        f.write(f"unique_pairs\t{len(pair_first_us)}\n")
        f.write(f"unique_triples\t{len(triple_first_us)}\n")
        f.write(f"max_requests_per_sec\t{int(df_requests_sec['requests'].max()) if not df_requests_sec.empty else 0}\n")
        f.write(f"max_requests_per_5sec\t{int(df_requests_5sec['requests'].max()) if not df_requests_5sec.empty else 0}\n")
        f.write(f"max_new_pairs_per_sec\t{int(df_new_pairs_sec['new_pairs'].max()) if not df_new_pairs_sec.empty else 0}\n")
        f.write(f"max_new_pairs_per_5sec\t{int(df_new_pairs_5sec['new_pairs'].max()) if not df_new_pairs_5sec.empty else 0}\n")
        f.write(f"max_new_triples_per_sec\t{int(df_new_triples_sec['new_triples'].max()) if not df_new_triples_sec.empty else 0}\n")
        f.write(f"max_new_triples_per_5sec\t{int(df_new_triples_5sec['new_triples'].max()) if not df_new_triples_5sec.empty else 0}\n")

    # Генерация датасета
    unique_triples_json_path = out_dir / "real_dataset.json"
    save_unique_triples_json(triple_first_us, unique_triples_json_path)

    # Plots

    # 1. Кумулятивное число уникальных пар
    plt.figure(figsize=(14, 5))
    plt.plot(df_cum_pairs["datetime"], df_cum_pairs["cumulative_unique_pairs"])
    plt.xlabel("Время, с")
    plt.ylabel("Число уникальных ключей (dst_MAC, VLAN)")
    plt.title("Общее число уникальных ключей (dst_MAC, VLAN)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(plots_dir / "cumulative_unique_pairs.png", dpi=150)
    plt.close()

    # 2. Кумулятивное число уникальных триплетов
    plt.figure(figsize=(14, 5))
    plt.plot(df_cum_triples["datetime"], df_cum_triples["cumulative_unique_triples"])
    plt.xlabel("Время, с")
    plt.ylabel("Число уникальных правил (dst_MAC, VLAN, dst_port)")
    plt.title("Общее число уникальных правил (dst_MAC, VLAN, dst_port)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(plots_dir / "cumulative_unique_triples.png", dpi=150)
    plt.close()

    # 3. Новые пары в секунду = insert/sec
    plt.figure(figsize=(14, 5))
    plt.step(df_new_pairs_sec["datetime"], df_new_pairs_sec["new_pairs"], where="post")
    plt.xlabel("Время, с")
    plt.ylabel("Новые ключи")
    plt.title("Частота появления новых ключей в секунду. Операция insert")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(plots_dir / "new_pairs_per_sec.png", dpi=150)
    plt.close()

    # 4. Новые пары в 5 секунд = insert/5sec
    plt.figure(figsize=(14, 5))
    plt.step(df_new_pairs_5sec["datetime"], df_new_pairs_5sec["new_pairs"], where="post")
    plt.xlabel("Время, с")
    plt.ylabel("Новые ключи")
    plt.title("Частота появления новых ключей в 5 секунд. Операция insert")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(plots_dir / "new_pairs_per_5sec.png", dpi=150)
    plt.close()

    # 5. Запросы в секунду = find/sec
    plt.figure(figsize=(14, 5))
    plt.step(df_requests_sec["datetime"], df_requests_sec["requests"], where="post")
    # add_mean_line(df_requests_sec["datetime"])
    plt.xlabel("Время, с")
    plt.ylabel("Операции поиска")
    plt.title("Число операций поиска в секунду")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(plots_dir / "requests_per_sec.png", dpi=150)
    plt.close()

    # 6. Запросы в 5 секунд = find/5sec
    plt.figure(figsize=(14, 5))
    plt.step(df_requests_5sec["datetime"], df_requests_5sec["requests"], where="post")
    plt.xlabel("Время, с")
    plt.ylabel("Операции поиска")
    plt.title("Число операций поиска в 5 секунд")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(plots_dir / "requests_per_5sec.png", dpi=150)
    plt.close()

    # Новые правила в секунду
    plt.figure(figsize=(14, 5))
    plt.step(df_new_triples_sec["datetime"], df_new_triples_sec["new_triples"], where="post")
    # add_mean_line(df_new_triples_sec["datetime"])
    plt.xlabel("Время, с")
    plt.ylabel("Новые правила")
    plt.title("Число появление новых правил в секунду. Операция upsert")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(plots_dir / "new_triples_per_sec.png", dpi=150)
    plt.close()


    # Новые правила в 5 секунд
    plt.figure(figsize=(14, 5))
    plt.step(df_new_triples_5sec["datetime"], df_new_triples_5sec["new_triples"], where="post")
    plt.xlabel("Время, с")
    plt.ylabel("Новые правила")
    plt.title("Число появление новых правил в 5 секунд. Операция upsert")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(plots_dir / "new_triples_per_5sec.png", dpi=150)
    plt.close()


    log("done")
    log(f"saved: {out_dir / 'cumulative_unique_pairs.tsv'}")
    log(f"saved: {out_dir / 'cumulative_unique_triples.tsv'}")
    log(f"saved: {out_dir / 'new_pairs_per_sec.tsv'}")
    log(f"saved: {out_dir / 'new_pairs_per_5sec.tsv'}")
    log(f"saved: {out_dir / 'requests_per_sec.tsv'}")
    log(f"saved: {out_dir / 'requests_per_5sec.tsv'}")
    log(f"saved: {summary_path}")


if __name__ == "__main__":
    main()