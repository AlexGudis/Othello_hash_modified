import pandas as pd
import matplotlib.pyplot as plt

# Загружаем данные из файла
df = pd.read_csv("new_macs_per_sec.tsv", sep="\t", header=None,
                 names=["timestamp", "new_macs"])

# Сортировка
df = df.sort_values("timestamp")

# Переводим timestamp в datetime
df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")

# --- Заполняем пропущенные секунды нулями ---

# Делаем индекс по времени
df = df.set_index("datetime")

# Создаём полный диапазон секунд
full_range = pd.date_range(start=df.index.min(),
                           end=df.index.max(),
                           freq="1s")

# Периндексация с заполнением нулями
df = df.reindex(full_range, fill_value=0)

# --- Строим график ---

plt.figure()
plt.step(df.index, df["new_macs"], where="post")

plt.xlabel("Время")
plt.ylabel("Число новых MAC адресов")
plt.title("Зависимость появления новых MAC адресов от времени")

plt.xticks(rotation=45)
plt.tight_layout()
plt.show()