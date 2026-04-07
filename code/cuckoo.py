import random, json
from common import get_keys, generate_kv, Info, test_info, draw
from abstracts import HashAlgorithmBase
from random import randint
from hash import FastHash
from time import time

class CuckooHash(HashAlgorithmBase):
    def __init__(self, m=8, *, seed1=None, seed2=None):
        super().__init__()
        self.m = m
        self.n = 0

        self.t1 = [None] * self.m
        self.t2 = [None] * self.m

        self.seed1 = random.getrandbits(64) if seed1 is None else seed1
        self.seed2 = random.getrandbits(64) if seed2 is None else seed2

        self._build_hashes()

    def _build_hashes(self) -> None:
        """Пересоздать хеш-функции под текущий размер таблиц."""
        self._h1 = FastHash(self.seed1, self.m)
        self._h2 = FastHash(self.seed2, self.m)

    @staticmethod
    def _key_to_int(key) -> int:
        """Преобразовать ключ к integer для fast hash.

        Поддерживаются:
        - int
        - str формата 'xx:xx:xx:xx:xx:xx-vlan'
        """
        if isinstance(key, int):
            return key

        if isinstance(key, str):
            return FastHash.convert_to_int_key(key)

        raise TypeError(f"Unsupported key type: {type(key)!r}")

    def __len__(self):
        return self.n

    def h1(self, key):
        return self._h1(self._key_to_int(key))

    def h2(self, key):
        return self._h2(self._key_to_int(key))

    def find(self, key):
        i = self.h1(key)
        self.metrics.inc("hash_calls_total")
        if self.t1[i] is not None and self.t1[i][0] == key:
            return self.t1[i][1]

        j = self.h2(key)
        self.metrics.inc("hash_calls_total")
        if self.t2[j] is not None and self.t2[j][0] == key:
            return self.t2[j][1]

        return None

    def delete(self, key):
        i = self.h1(key)
        self.metrics.inc("hash_calls_total")
        if self.t1[i] is not None and self.t1[i][0] == key:
            self.t1[i] = None
            self.n -= 1
            return True

        j = self.h2(key)
        self.metrics.inc("hash_calls_total")
        if self.t2[j] is not None and self.t2[j][0] == key:
            self.t2[j] = None
            self.n -= 1
            return True

        return False

    def insert(self, key, value):
        if self.find(key) is not None:
            return False

        # Суммарная загрузка <= 0.5
        if self.n >= self.m:
            self.resize(2 * self.m)

        cur = (key, value)
        table = 1

        for _ in range(2 * self.m):
            if table == 1:
                i = self.h1(cur[0])
                self.metrics.inc("hash_calls_total")

                if self.t1[i] is None:
                    self.t1[i] = cur
                    self.n += 1
                    return True

                cur, self.t1[i] = self.t1[i], cur
                table = 2

            else:
                j = self.h2(cur[0])
                self.metrics.inc("hash_calls_total")

                if self.t2[j] is None:
                    self.t2[j] = cur
                    self.n += 1
                    return True

                cur, self.t2[j] = self.t2[j], cur
                table = 1

        # Если попали в цикл — увеличиваем таблицу и вставляем заново
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

        self._build_hashes()

        for key, value in old_items:
            self.insert(key, value)

    def __str__(self):
        return f"T1={self.t1}\nT2={self.t2}"

    

if __name__ == '__main__':
    with open('mac_vlan_mapping.json', 'r') as JSON:
        json_dict = json.load(JSON)

    keys, values = get_keys(json_dict)

    # Каждая таблица как размер всех текущих ключей
    cuko = CuckooHash(len(json_dict))

    ready_to_insert = len(json_dict)
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

    start_t = time()
    for i in range(50_000):
        cuko.find(keys[randint(1, len(keys) - 1)])
    end_t = time()
    print(f"Время поиска {end_t - start_t}")
    




            

