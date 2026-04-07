import random
import matplotlib.pyplot as plt
from common import generate_kv
from cuckoo import CuckooHash


# ============================================================
# 4. Генераторы нагрузок
# ============================================================

def make_insert_workload(size: int):
    return [("insert", *generate_kv()) for _ in range(size)]


# ============================================================
# 5. Раннер
# ============================================================

class BenchmarkRunner:
    def run(self, algorithm, ops):
        algorithm.reset_metrics()

        for op, key, value in ops:
            if op == "insert":
                algorithm.insert(key, value)
            elif op == "find":
                algorithm.find(key)
            elif op == "delete":
                algorithm.delete(key)
            else:
                raise ValueError(f"Unknown operation: {op}")

        return algorithm.metrics_snapshot()


# ============================================================
# 6. Эксперимент
# ============================================================

def experiment(algorithm_factory, sizes):
    runner = BenchmarkRunner()

    x_sizes = []
    y_insert_total = []

    for n in sizes:
        algo = algorithm_factory(n)

        # 1) только вставки
        insert_ops = make_insert_workload(n + int(n * 0.1))
        m_insert = runner.run(algo, insert_ops)

        x_sizes.append(n)
        y_insert_total.append(m_insert["hash_calls_total"])

        print(
            f"N={n:5d} | "
            f"insert={m_insert['hash_calls_total']:7d} | "
        )

    return {
        "sizes": x_sizes,
        "insert_hash_calls": y_insert_total,
    }


# ============================================================
# 7. График
# ============================================================

def plot_results(results):
    sizes = results["sizes"]

    plt.figure(figsize=(9, 5))
    plt.plot(sizes, results["insert_hash_calls"], marker="o", label="insert workload")
    #plt.plot(sizes, results["find_hash_calls"], marker="o", label="find workload")
    #plt.plot(sizes, results["mixed_hash_calls"], marker="o", label="mixed workload")

    plt.xlabel("Размер множества ключей")
    plt.ylabel("Число вызовов хеш-функций")
    plt.title("Отношение числа вызовов хеш-функций к числу операций вставки")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.show()


# ============================================================
# 8. Точка входа
# ============================================================

if __name__ == "__main__":
    random.seed(42)

    sizes = [100, 200, 500, 1000, 2000, 4000, 10000]

    results = experiment(
        algorithm_factory=CuckooHash,
        sizes=sizes,
    )

    plot_results(results)