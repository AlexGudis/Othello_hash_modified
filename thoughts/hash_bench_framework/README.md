# Hash benchmark framework

Мини-фреймворк для сравнения нескольких алгоритмов хеширования по общему интерфейсу.

## Что внутри

- `interfaces.py` — общий интерфейс хеш-таблицы
- `metrics.py` — observer событий и функции статистики
- `algorithms/cuckoo.py` — пример реализации cuckoo hash
- `workloads.py` — генераторы нагрузок
- `runner.py` — универсальный раннер и сохранение CSV
- `plots.py` — базовые функции построения графиков
- `example_main.py` — готовый пример запуска

## Универсальные black-box метрики

- throughput (`ops/s`)
- latency (`avg`, `p50`, `p95`, `p99`, `max`)
- load factor
- approximate memory usage

## Универсальные white-box события

Алгоритм может сигналить их через `observer.event(...)`:

- `memory_probe`
- `memory_write`
- `collision`
- `relocation`
- `resize`
- `cycle_detected`
- `hash_calls_total`
- `hash_calls_h1`
- `hash_calls_h2`

## Как подключить новый алгоритм

1. Реализовать интерфейс из `interfaces.py`
2. По желанию добавлять события в observer
3. Добавить factory-функцию в `example_main.py`

## Запуск

```bash
PYTHONHASHSEED=0 python example_main.py
```

Результаты:

- `results/benchmark_results.csv`
- `results/benchmark_summary.csv`
- `results/plots/*.png`
