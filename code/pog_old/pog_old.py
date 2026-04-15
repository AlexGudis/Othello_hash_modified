from othello import Othello
import bitarray
import hashlib
from common import Info
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from abstracts import HashAlgorithmBase

# TODO: Вероятно, от этого в принципе придется отказаться, так как Отелло будет только одно в реальности. 
# Отказ от фиктивной параллельности



class POG_OLD(HashAlgorithmBase):
    def __init__(self, table):
        super().__init__()
        self.group = []
        self.table = table
        self.construct()


    def find(self, key):
        info = Info('pog.search')
        ans = ''
        for i in range(len(self.group)):
            bit, info_oth = self.group[i].search(key)
            ans += str(bit)
            info.hash += info_oth.hash
            info.memory += info_oth.memory

            metrics_snap = self.group[i].metrics_snapshot()
            for k, v in self.metrics.counters.items():
                self.metrics.inc(k, metrics_snap[k])
            

        #print(f'FOUND = {ans}')

        return int(ans, 2), info

    def generate_table(self, table, cnt, i):
        specific_table = table.copy()
        for k, v in specific_table.items():
            new_v = bin(int(v))[2:]
            if len(new_v) != cnt:
                new_v = '0' * (cnt - len(new_v)) + new_v
                
            #print(new_v, len(new_v))
            #print(f'Current othello = {i}, t_k for key {k} and value {bin(int(v))[2:]} is {new_v[i]}')

            specific_table[k] = new_v[i]
        return specific_table


    def construct(self):

        # Определяем число Отелло структур
        max_port = max(int(v) for k,v in self.table.items())
        #print(max_port)
        cnt = len(bin(max_port)[2:])
        #print(cnt)

        for i in range(cnt):
            n = len(self.table)

            # LOADFACTOR = +-40%
            a = bitarray.bitarray(int(n * 1.33)) 
            b = bitarray.bitarray(int(n * 1.33))
            ma = len(a)
            mb = len(b)
            ha = hashlib.sha3_512
            hb = hashlib.sha256

            oth = Othello(ma, mb, ha, hb, a, b)

            specific_table = self.generate_table(self.table, cnt, i)
            
            #print(specific_table)

            oth.construct(specific_table)

            self.group.append(oth)

            metrics_snap = self.group[i].metrics_snapshot()
            for k, v in self.metrics.counters.items():
                self.metrics.inc(k, metrics_snap[k])

    def insert(self, key, value):
        info = Info(type='oth_pog.insert')

        cnt = len(self.group)

        new_v = bin(int(value))[2:]
        if len(new_v) != cnt:
            new_v = '0' * (cnt - len(new_v)) + new_v

        
        for i in range(cnt):
            #print(f'In pog insert: {k}, {new_v[i]}')
            specific_table = self.generate_table(self.table, cnt, i)
            info_oth = self.group[i].insert(specific_table, key, new_v[i])
            info.memory += info_oth.memory
            info.hash += info_oth.hash

            metrics_snap = self.group[i].metrics_snapshot()
            for k, v in self.metrics.counters.items():
                self.metrics.inc(k, metrics_snap[k])

        self.table[key] = value
        return info
            
    def delete(self, key):
        info = Info(type='pog.delete')
        
        for i in range(len(self.group)):
            info_oth = self.group[i].delete(key)

            info.memory += info_oth.memory
            info.hash += info_oth.hash

            metrics_snap = self.group[i].metrics_snapshot()
            for k, v in self.metrics.counters.items():
                self.metrics.inc(k, metrics_snap[k])
        return info