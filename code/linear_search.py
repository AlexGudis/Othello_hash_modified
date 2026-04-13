from abstracts import HashAlgorithmBase
from time import time
import json
from common import get_keys
from random import randint

class LinearSearchTable(HashAlgorithmBase):
    def __init__(self, m=8):
        super().__init__()
        self.m = m
        self.current_items_count = 0
        self.items = [None] * self.m

    def __len__(self):
        return self.current_items_count

    def find(self, key):
        for item in self.items:
            self.metrics.inc("memory_count")
            if item is not None and item[0] == key:
                return item[1]
        return None

    def delete(self, key):
        for i in range(self.m):
            self.metrics.inc("memory_count")
            item = self.items[i]
            if item is not None and item[0] == key:
                self.items[i] = None
                self.current_items_count -= 1
                return True
        return False

    def insert(self, key, value):
        # Проверка на дубликат
        if self.find(key) is not None:
            return False

        # Если места нет — увеличиваем массив
        if self.current_items_count >= self.m:
            self.resize(2 * self.m)

        # Ищем первую свободную ячейку
        for i in range(self.m):
            self.metrics.inc("memory_count")
            if self.items[i] is None:
                self.items[i] = (key, value)
                self.metrics.inc("memory_count")
                self.current_items_count += 1
                return True

        # На всякий случай
        self.resize(2 * self.m)
        return self.insert(key, value)

    def resize(self, new_m):
        old_items = []

        for item in self.items:
            self.metrics.inc("memory_count")
            if item is not None:
                old_items.append(item)

        self.m = new_m
        self.items = [None] * self.m
        self.current_items_count = 0

        for key, value in old_items:
            self.insert(key, value)

    def __str__(self):
        return f"LinearTable={self.items}"
    


if __name__ == '__main__':
    with open('mac_vlan_mapping.json', 'r') as JSON:
        json_dict = json.load(JSON)

    keys, values = get_keys(json_dict)

    # Каждая таблица как размер всех текущих ключей
    linear = LinearSearchTable(len(json_dict))

    ready_to_insert = len(json_dict)
    inserted = 0
    find_after = 0

    for i in range(ready_to_insert):
        # print(f"Insterting {keys[i], values[i]}")
        if linear.insert(keys[i], values[i]):
            inserted += 1

        ans = linear.find(keys[i])
        if ans == values[i]:
            find_after += 1
        else:
            print(f'ERROR with k-v pair: {keys[i]}---{values[i]}')
    
    print(f'Correct is {find_after} of {ready_to_insert}')
    print(f'Final size is {len(linear)}')

    start_t = time()
    for i in range(50_000):
        linear.find(keys[randint(1, len(keys) - 1)])
    end_t = time()
    print(f"Время поиска {end_t - start_t}")