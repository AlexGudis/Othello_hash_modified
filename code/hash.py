"""Ramakrishna M., Fu E., Bahcekapili E. A performance study of hashing functions for
hardware applications // Proc. of Int. Conf. on Computing and Information. — Citeseer.
1994. — С. 1621—1636."""

import numpy as np
rng = np.random.default_rng(seed=42)

class HashFunction:
    def __init__(self, key_dim, hash_dim, max_val=None, *, salt=None, mat=None):
        self.dims = key_dim, hash_dim
        if mat is None:
            mat = rng.integers(2, size=self.dims, dtype=bool)
        if salt is None:
            salt = rng.integers(2, size=self.dims[0], dtype=bool)
        if max_val == 1 << hash_dim:
            max_val = None
        self.max_val = max_val
        self.pows = 1 << np.arange(key_dim), 1 << np.arange(hash_dim)
        self.mat = np.array(mat)
        self.salt = np.array(salt)

    """def __call__(self, key: int):
        res = (key & self.pows[0]) > 0
        res = (res ^ self.salt).astype(int)
        res = np.dot(res, self.mat) % 2
        res = int((res * self.pows[1]).sum())
        if self.max_val is not None:
            res = (res * self.max_val) >> self.dims[1]
        return res"""
    

    def __call__(self, key: int):
        # 1) Жёстко ограничим ключ до key_dim бит (заодно спасает от "случайно больше 60 бит")
        key &= (1 << self.dims[0]) - 1

        # 2) Достаём биты как 0/1: ((key >> i) & 1)
        idx = np.arange(self.dims[0], dtype=np.uint64)
        v = ((key >> idx) & 1).astype(np.uint8)          # shape (n,)

        # 3) XOR с солью
        x = v ^ self.salt.astype(np.uint8)               # (n,)

        # 4) умножение на матрицу по mod 2
        y = (x @ self.mat.astype(np.uint8)) & 1          # (m,)

        # 5) собрать в int
        res = int((y.astype(np.uint64) * self.pows[1]).sum())

        # 6) опциональное сжатие в [0, max_val)
        if self.max_val is not None:
            res = (res * self.max_val) >> self.dims[1]
        return res

    
    @staticmethod
    def convert_to_int_key(key: str) -> int:
        """
        Преобразовать ключ-строку вида xx:xx:xx:xx:xx:xx-xxx к 60-битному числу,
            где x - 4 бита и представляет MAC-VLAN пару
        """
        mac, vlan = key.split('-')
        mac = ''.join(mac.split(':'))
        key = int(mac + vlan, 16)
        # print(key)
        return key


if __name__ == "__main__":
    hf1 = HashFunction(60, 4, 10)
    print(hf1(HashFunction.convert_to_int_key("9C:A4:3B:8D:B4:35-906")))
    HashFunction.convert_to_int_key("9C:A4:3B:8D:B4:35-905")

