"""Здесь будут проводиться тестирования Отелло хеширования при загрузке таблицы в 90%"""
"""Сравнительное тестирование времени работы операции поиска"""

import numpy as np
from common import get_keys, generate_mac, generate_vlan, generate_kv, draw, test_info
import time
import random
import json
import pog
import matplotlib.pyplot as plt


def draw(
    filepath,
    title_prefix,
    labels=("вставка", "удаление", "поиск"),
    ylabel="Среднее значение",
):
    # Чтение и парсинг данных
    load_factors = []
    insert_vals = []
    delete_vals = []
    search_vals = []

    with open(filepath, "r") as file:
        for line in file:
            parts = line.strip().split()
            if len(parts) != 4:
                continue
            load, insert, delete, search = map(float, parts)
            load_factors.append(load)
            insert_vals.append(insert)
            delete_vals.append(delete)
            search_vals.append(search)

    # Список всех серий: (значения, подпись, цвет, маркер)
    data_series = [
        (insert_vals, labels[0], 'blue', 'o'),
        (delete_vals, labels[1], 'red', 's'),
        (search_vals, labels[2], 'green', '^')
    ]

    any_plotted = False

    for vals, label, color, marker in data_series:
        if any(val != 0 for val in vals):
            plt.figure(figsize=(10, 4))
            plt.plot(load_factors, vals, marker=marker, color=color, label=label)

            y_min, y_max = min(vals), max(vals)
            if y_max - y_min > 0:
                plt.ylim(y_min - 0.1 * (y_max - y_min), y_max + 0.1 * (y_max - y_min))

            #plt.yscale("log")
            plt.title(f"{title_prefix}{label}")
            plt.xlabel("Фактор загрузки")
            plt.ylabel(ylabel)
            plt.grid(True)
            plt.legend()
            plt.tight_layout()
            plt.show()
            any_plotted = True

    if not any_plotted:
        print(f"[!] Все значения в файле {filepath} равны нулю — графики не построены.")


draw(filepath='data/othello_load/othello_load_memory', title_prefix='Среднее число обращений к памяти для операции: ')
draw(filepath='data/othello_load/othello_load_hash', title_prefix='Среднее число вызовов хеш-функций для операции: ')
draw(filepath='data/othello_load/othello_load_time', title_prefix='Среднее время работы для операции: ')
'''


def test_correct(oth, json_dict, keys):
    cnt = 0
    for i in range(len(json_dict)):
        key = keys[i]
        ans = oth.search(key)[0]

        # print(ans,json_dict[key], key)
        if str(json_dict[key]) == str(ans):
            cnt += 1

    return cnt


with open('mac_vlan_mapping.json', 'r') as JSON:
    json_dict = json.load(JSON)

keys, values = get_keys(json_dict)
pg = pog.POG()
pg.construct(json_dict)

"""Вычисление, сколько элементов надо добавить для текущих размеров Отелло, чтобы загрузка стала определенной"""
max_load = pg.group[0].ma
load = len(json_dict) / max_load
while load < 0.9:
    # Добавляем недостающее число правил, чтобы загрузка каждой доли графа стала фиксированной
    
    print(f'max_load = {max_load}, current_load = {len(json_dict)}, load_factor = {load}')
    for i in range(int((max_load * load - len(json_dict)))):
        new_k, new_v = generate_kv()
        info = pg.insert(json_dict, new_k, new_v)
        # print(f'{i}/{int(max_load * 0.9 - n)} ВСТАВКА. Кол-во обращений к памяти = {info.memory}')
        json_dict[new_k] = new_v

    print(f'Текущая загрузка доли графа: {len(json_dict)} / {max_load}')
    keys, values = get_keys(json_dict)
    cnt = test_correct(pg, json_dict, keys)
    print(f'Correct is {cnt} of {len(json_dict)}')
    load += 0.01


    """Тестирование среднего числа обращений к памяти и вызовов хеш-функции на операции ВСТАВКИ при 90% загрузке"""
    """Тестирование среднего числа обращений к памяти и вызовов хеш-функции на операции УДАЛЕНИЕ при 90% загрузке"""
    insert_memory_cnt = []
    insert_hash_cnt = []
    insert_time = []
    delete_time = []

    delete_mem_cnt = []
    delete_hash_cnt = []
    for _ in range(100):
        new_k, new_v = generate_kv()

        start_t = time.time()
        info_ins = pg.insert(json_dict, new_k, new_v)
        finish_t = time.time()
        insert_memory_cnt.append(info_ins.memory)
        insert_hash_cnt.append(info_ins.hash)
        insert_time.append(finish_t - start_t)

        start_t = time.time()
        info_del = pg.delete(new_k)
        finish_t = time.time()
        delete_mem_cnt.append(info_del.memory)
        delete_hash_cnt.append(info_del.hash)
        delete_time.append(finish_t - start_t)


    """Тестирование среднего числа обращений к памяти и вызовов хеш-функции на операции ПОИСКА при 90% загрузке"""
    search_memory_cnt = []
    search_hash_cnt = []
    search_time = []
    for i in range(len(json_dict)):
        key = keys[i]
        start_t = time.time()
        ans, info = pg.search(key)
        finish_t = time.time()
        search_time.append(finish_t - start_t)
        search_memory_cnt.append(info.memory)
        search_hash_cnt.append(info.hash)
        # print(ans,json_dict[key], key)
        if str(json_dict[key]) == str(ans):
            cnt += 1

    avg_insert_mem = sum(insert_memory_cnt) / len(insert_memory_cnt)
    avg_delete_mem = sum(delete_mem_cnt) / len(delete_mem_cnt)
    avg_search_mem = sum(search_memory_cnt) / len(search_memory_cnt)

    avg_insert_hash = sum(insert_hash_cnt) / len(insert_hash_cnt)
    avg_delete_hash = sum(delete_hash_cnt) / len(delete_hash_cnt)
    avg_search_hash = sum(search_hash_cnt) / len(search_hash_cnt)

    avg_insert_time = sum(insert_time) / len(insert_time)
    avg_delete_time = sum(delete_time) / len(delete_time)
    avg_search_time = sum(search_time) / len(search_time)


    data = {'avg_insert_mem': avg_insert_mem,
            'avg_delete_mem': avg_delete_mem, 'avg_search_mem': avg_search_mem, 
            'avg_insert_hash': avg_insert_hash, 'avg_delete_hash':avg_delete_hash,
            'avg_search_hash':avg_search_hash, 'avg_insert_time':avg_insert_time,
            'avg_delete_time':avg_delete_time, 'avg_search_time': avg_search_time}


    with open('data/othello_load/othello_load_memory', 'a') as f:
        f.write(f'{load} {avg_insert_mem} {avg_delete_mem} {avg_search_mem}\n')
    
    with open('data/othello_load/othello_load_hash', 'a') as f:
        f.write(f'{load} {avg_insert_hash} {avg_delete_hash} {avg_search_hash}\n')

    with open('data/othello_load/othello_load_time', 'a') as f:
        f.write(f'{load} {avg_insert_time} {avg_delete_time} {avg_search_time}\n')


keys, values = get_keys(json_dict)
cnt = test_correct(pg, json_dict, keys)
print(f'Correct is {cnt} of {len(json_dict)}')


draw(filepath='data/othello_load/othello_load_memory', title_prefix='Среднее число обращений к памяти для операции: ')
draw(filepath='data/othello_load/othello_load_hash', title_prefix='Среднее число вызовов хеш-функций для операции: ')
draw(filepath='data/othello_load/othello_load_time', title_prefix='Среднее время работы для операции: ')

'''