import random
import matplotlib.pyplot as plt
import numpy as np


def test_info(avg_insert_mem, avg_delete_mem, avg_search_mem, avg_insert_hash, avg_delete_hash, avg_search_hash, avg_insert_time):
    print(f'AVG mem_cnt on insert = {avg_insert_mem}')
    print(f'AVG hash_cnt on insert = {avg_insert_hash}')
    print(f'AVG TIME on insert = {avg_insert_time}\n')

    print(f'AVG mem_cnt on search = {avg_search_mem}')
    print(f'AVG hash_cnt on search = {avg_search_hash}\n')

    print(f'AVG mem_cnt on delete = {avg_delete_mem}')
    print(f'AVG hash_cnt on delete = {avg_delete_hash}\n')


def get_data(filename):
    data = dict()
    with open(filename, "r") as f:
        for el in f.readlines():
            el = el.split()
            data[el[0]] = float(el[1])
    return data


def draw():
    oth_data = get_data('othello_data')
    cucko_data = get_data('cuckoo_data')

    avg_data = {
        'insert_mem': [cucko_data['avg_insert_mem'], oth_data['avg_insert_mem']],
        'delete_mem': [cucko_data['avg_delete_mem'], oth_data['avg_delete_mem']],
        'search_mem': [cucko_data['avg_search_mem'], oth_data['avg_search_mem']],
        'insert_hash': [cucko_data['avg_insert_hash'], oth_data['avg_insert_hash']],
        'delete_hash': [cucko_data['avg_delete_hash'], oth_data['avg_delete_hash']],
        'search_hash': [cucko_data['avg_search_hash'], oth_data['avg_search_hash']],
        'insert_time': [cucko_data['avg_insert_time'], oth_data['avg_insert_time']],
        'delete_time': [cucko_data['avg_delete_time'], oth_data['avg_delete_time']],
        'search_time': [cucko_data['avg_search_time'], oth_data['avg_search_time']]
    }

    labels = ['Cuckoo', 'Othello']
    bar_width = 0.2
    x = np.arange(len(labels))

    def clean_log_values(values, min_value=1e-6):
        return [v if v > 0 else min_value for v in values]

    def plot_comparison(title, y_label, keys, colors, filename, use_log_scale=False):
        plt.figure(figsize=(9, 5))
        for i, key in enumerate(keys):
            offset = (i - 1) * bar_width  # -0.2, 0, +0.2
            values = avg_data[key]
            bars = plt.bar(x + offset, values, width=bar_width,
                           label=key.split('_')[0].capitalize(), color=colors[i])

            # Подписи сверху
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2, height + height*0.03,
                         f'{height:.6f}' if height < 0.001 else f'{height:.3f}', ha='center', va='bottom', fontsize=9)

        plt.xticks(x, labels)
        plt.ylabel(y_label)
        plt.title(title)
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.4)
        plt.tight_layout()

        if use_log_scale:
            plt.yscale('log')
            max_y = max(max(avg_data[k]) for k in keys)
            min_y = min(min(avg_data[k]) for k in keys)
            # увеличиваем "потолок" в 10 раз
            plt.ylim(bottom=0.01, top=max_y * 2)
        else:
            plt.ylim(0, max(max(avg_data[k]) for k in keys) * 1.25)
        plt.savefig(f'{filename}.png')
        plt.show()

    for key in avg_data:
        avg_data[key] = clean_log_values(avg_data[key])

    print(avg_data)

    # 1. Обращения к памяти
    plot_comparison(
        title='Среднее число обращений к памяти при 20% загрузке',
        y_label='Обращения к памяти',
        keys=['insert_mem', 'delete_mem', 'search_mem'],
        colors=['skyblue', 'salmon', 'mediumseagreen'],
        filename='./images/memory',
        use_log_scale=True
    )

    # 2. Вызовы хеш-функций
    plot_comparison(
        title='Среднее число вызовов хеш-функции при 20% загрузке',
        y_label='Вызовы хеш-функции',
        keys=['insert_hash', 'delete_hash', 'search_hash'],
        colors=['skyblue', 'salmon', 'mediumseagreen'],
        filename='./images/hash',
        use_log_scale=True
    )

    # 3. Время выполнения
    plot_comparison(
        title='Среднее время выполнения операций при 20% загрузке',
        y_label='Время (сек)',
        keys=['insert_time', 'delete_time', 'search_time'],
        colors=['skyblue', 'salmon', 'mediumseagreen'],
        filename='./images/time'
    )


def get_keys(json_dict):
    keys = []
    values = []
    for k, v in json_dict.items():
        keys.append(k)
        values.append(v)
    return keys, values


def generate_mac():
    """Генерирует случайный MAC-адрес."""
    return ':'.join(f"{random.randint(0, 255):02X}" for _ in range(6))


def generate_vlan():
    """Генерирует случайный VLAN ID (от 1 до 4095)."""
    return random.randint(1, 4095)


def generate_kv():
    mac = generate_mac()
    vlan = generate_vlan()
    key = f"{mac}-{vlan}"
    value = str(random.randint(0, 5))  # Порт
    return key, value


class Info():
    def __init__(self, type='', records=0, key_inc=False, memory=0, hash=0, failed=0, contruct_cnt=0):
        self.type = type
        self.records = records
        self.key_inc = key_inc
        self.memory = memory   # число доступов к памяти
        self.hash = hash   # число вычислений хеш-функций
        self.failed = failed
        # число перестроений структуры для операции insert в Отелло
        self.contruct_cnt = contruct_cnt


if __name__ == '__main__':
    draw()
