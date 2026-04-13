"""Ramakrishna M., Fu E., Bahcekapili E. A performance study of hashing functions for
hardware applications // Proc. of Int. Conf. on Computing and Information. — Citeseer.
1994. — С. 1621—1636."""

import numpy as np

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



'''class FastHash:
    def __init__(self, seed: int, max_val: int):
        self.seed = seed & 0xFFFFFFFFFFFFFFFF
        self.max_val = max_val

    def __call__(self, key: int) -> int:
        x = (key ^ self.seed) & 0xFFFFFFFFFFFFFFFF

        x ^= (x >> 30)
        x = (x * 0xbf58476d1ce4e5b9) & 0xFFFFFFFFFFFFFFFF
        x ^= (x >> 27)
        x = (x * 0x94d049bb133111eb) & 0xFFFFFFFFFFFFFFFF
        x ^= (x >> 31)

        return x % self.max_val
    

    @staticmethod
    def convert_to_int_key(key: str) -> int:
        """Преобразовать ключ MAC-VLAN в 60-битное число.

        Формат ключа: 'xx:xx:xx:xx:xx:xx-vlan'
        где MAC занимает 48 бит, VLAN — 12 бит.
        """
        mac_str, vlan_str = key.split('-')

        mac_int = int(mac_str.replace(':', ''), 16)
        vlan_int = int(vlan_str, 10)

        if not (0 <= mac_int < (1 << 48)):
            raise ValueError(f"MAC out of range: {mac_str}")

        if not (0 <= vlan_int < (1 << 12)):
            raise ValueError(f"VLAN out of range [0, 4095]: {vlan_int}")

        return (mac_int << 12) | vlan_int'''
    

import hashlib
class FastHash:
    """хеш для байтового ключа MAC-VLAN."""

    def __init__(self, seed: int, size: int) -> None:
        self.size = size
        self.key = seed.to_bytes(8, byteorder="little", signed=False)
    def __call__(self, key_encoded: bytes) -> int:
        digest = hashlib.blake2b(
            key_encoded,
            digest_size=8,
            key=self.key,
        ).digest()

        h = int.from_bytes(digest, byteorder="little", signed=False)
        return h % self.size

    @staticmethod
    def convert_to_int_key(key: str) -> bytes:
        return key.strip().lower().encode("utf-8")


if __name__ == "__main__":
    hf1 = HashFunction(60, 4, 10)
    print(hf1(HashFunction.convert_to_int_key("98:EA:4E:BD:BF:37-4075")))
    print(hf1(HashFunction.convert_to_int_key("F7:9B:C2:01:8F:4D-684")))
    HashFunction.convert_to_int_key("9C:A4:3B:8D:B4:35-905")

