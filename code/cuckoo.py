import random, json
from common import get_keys, generate_kv, Info, test_info, draw

load = 35

class CuckooHash:
    def __init__(self, m=8):
        self.m = max(2, m)          # размер каждой из двух таблиц
        self.n = 0                  # число элементов
        self.t1 = [None] * self.m
        self.t2 = [None] * self.m

    def __len__(self):
        return self.n

    def h1(self, key):
        return hash(key) % self.m

    def h2(self, key):
        return hash(("x", key)) % self.m

    def find(self, key):
        i = self.h1(key)
        if self.t1[i] is not None and self.t1[i][0] == key:
            return self.t1[i][1]

        j = self.h2(key)
        if self.t2[j] is not None and self.t2[j][0] == key:
            return self.t2[j][1]

        return None

    def delete(self, key):
        i = self.h1(key)
        if self.t1[i] is not None and self.t1[i][0] == key:
            self.t1[i] = None
            self.n -= 1
            return True

        j = self.h2(key)
        if self.t2[j] is not None and self.t2[j][0] == key:
            self.t2[j] = None
            self.n -= 1
            return True

        return False

    def insert(self, key, value):
        if self.find(key) is not None:
            return False

        # суммарная загрузка <= 0.5
        if self.n >= self.m:
            self.resize(2 * self.m)

        cur = (key, value)
        table = 1

        for _ in range(2 * self.m):
            if table == 1:
                i = self.h1(cur[0])

                if self.t1[i] is None:
                    self.t1[i] = cur
                    self.n += 1
                    return True

                cur, self.t1[i] = self.t1[i], cur
                table = 2

            else:
                j = self.h2(cur[0])

                if self.t2[j] is None:
                    self.t2[j] = cur
                    self.n += 1
                    return True

                cur, self.t2[j] = self.t2[j], cur
                table = 1

        # если попали в цикл — увеличиваем таблицу и вставляем заново
        self.resize(2 * self.m)
        return self.insert(cur[0], cur[1])

    def resize(self, new_m):
        old_items = []

        for x in self.t1:
            if x is not None:
                old_items.append(x)

        for x in self.t2:
            if x is not None:
                old_items.append(x)

        self.m = new_m
        self.t1 = [None] * self.m
        self.t2 = [None] * self.m
        self.n = 0

        for key, value in old_items:
            self.insert(key, value)

    def __str__(self):
        return f"T1={self.t1}\nT2={self.t2}"


def test():
    with open('mac_vlan_mapping.json', 'r') as JSON:
        json_dict = json.load(JSON)

    keys, values = get_keys(json_dict)

    # Каждая таблица как размер всех текущих ключей
    cuko = CuckooHash(len(json_dict))

    ready_to_insert = len(json_dict) % 7
    inserted = 0
    find_after = 0

    for i in range(ready_to_insert):
        print(f"Insterting {keys[i], values[i]}")
        if cuko.insert(keys[i], values[i]):
            inserted += 1

        ans = cuko.find(keys[i])
        if ans == values[i]:
            find_after += 1
        else:
            print(f'ERROR with k-v pair: {keys[i]}---{values[i]}')
    
    print(f'Correct is {find_after} of {ready_to_insert}')
    print(f'Final size is {len(cuko)}')


    # """Тестирование среднего числа обращений к памяти и вызовов хеш-функции на операции ВСТАВКИ"""
    # """Тестирование среднего числа обращений к памяти и вызовов хеш-функции на операции УДАЛЕНИЕ"""
    # insert_memory_cnt = []
    # insert_hash_cnt = []
    # insert_time = []

    # delete_mem_cnt = []
    # delete_hash_cnt = []
    # delete_time = []
    # for _ in range(100):
    #     new_k, new_v = generate_kv()

    #     start_t = time.time()
    #     res, info_ins = cuko.insert(new_k, new_v)
    #     finish_t = time.time()
    #     insert_memory_cnt.append(info_ins.memory)
    #     insert_hash_cnt.append(info_ins.hash)
    #     insert_time.append(finish_t - start_t)

    #     start_t = time.time()
    #     res, info_del = cuko.delete(new_k)
    #     finish_t = time.time()
    #     delete_mem_cnt.append(info_del.memory)
    #     delete_hash_cnt.append(info_del.hash)
    #     delete_time.append(finish_t - start_t)


    # """Тестирование среднего числа обращений к памяти и вызовов хеш-функции на операции ПОИСКА"""
    # search_memory_cnt = []
    # search_hash_cnt = []
    # search_time = []
    # cnt = 0
    # for i in range(load):
    #     start_t = time.time()
    #     ans, info = cuko.find(keys[i])
    #     finish_t = time.time()
    #     search_time.append(finish_t - start_t)
    #     search_memory_cnt.append(info.memory)
    #     search_hash_cnt.append(info.hash)
    #     if ans == values[i]:
    #         cnt += 1

    # avg_insert_mem = sum(insert_memory_cnt) / len(insert_memory_cnt)
    # avg_delete_mem = sum(delete_mem_cnt) / len(delete_mem_cnt)
    # avg_search_mem = sum(search_memory_cnt) / len(search_memory_cnt)

    # avg_insert_hash = sum(insert_hash_cnt) / len(insert_hash_cnt)
    # avg_delete_hash = sum(delete_hash_cnt) / len(delete_hash_cnt)
    # avg_search_hash = sum(search_hash_cnt) / len(search_hash_cnt)

    # avg_insert_time = sum(insert_time) / len(insert_time)
    # avg_delete_time = sum(delete_time) / len(delete_time)
    # avg_search_time = sum(search_time) / len(search_time)


    # data = {'avg_insert_mem': avg_insert_mem,
    #     'avg_delete_mem': avg_delete_mem, 'avg_search_mem': avg_search_mem, 
    #     'avg_insert_hash': avg_insert_hash, 'avg_delete_hash':avg_delete_hash,
    #     'avg_search_hash':avg_search_hash, 'avg_insert_time':avg_insert_time,
    #     'avg_delete_time':avg_delete_time, 'avg_search_time': avg_search_time}


    # with open('cuckoo_data', 'w+') as f:
    #     for k, v in data.items():
    #         f.writelines(f'{k} {v}\n')


    # test_info(avg_insert_mem, avg_delete_mem, avg_search_mem, avg_insert_hash, avg_delete_hash, avg_search_hash, avg_insert_time)

    # print(f'Correct is {cnt} of {load}')

    
if __name__ == '__main__':
    test()
            

