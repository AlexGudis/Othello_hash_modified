from othello import Othello
import bitarray
import hashlib
from common import Info

# TODO: Вероятно, от этого в принципе придется отказаться, так как Отелло будет только одно в реальности. 
# Отказ от фиктивной параллельности

class POG:
    def __init__(self):
        self.group = []


    def search(self, key):
        info = Info('pog.search')
        ans = ''
        for i in range(len(self.group)):
            bit, info_oth = self.group[i].search(key)
            ans += str(bit)
            info.hash += info_oth.hash
            info.memory += info_oth.memory

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


    def construct(self, table):

        # Определяем число Отелло структур
        max_port = max(int(v) for k,v in table.items())
        #print(max_port)
        cnt = len(bin(max_port)[2:])
        #print(cnt)

        for i in range(cnt):
            n = len(table)

            # LOADFACTOR = +-40%
            a = bitarray.bitarray(int(n * 5)) 
            b = bitarray.bitarray(int(n * 5))
            ma = len(a)
            mb = len(b)
            ha = hashlib.sha3_512
            hb = hashlib.sha256

            oth = Othello(ma, mb, ha, hb, a, b)

            specific_table = self.generate_table(table, cnt, i)
            
            #print(specific_table)

            oth.construct(specific_table)

            self.group.append(oth)

    def insert(self, table, k, v):
        info = Info(type='oth_pog.insert')

        cnt = len(self.group)

        new_v = bin(int(v))[2:]
        if len(new_v) != cnt:
            new_v = '0' * (cnt - len(new_v)) + new_v

        
        for i in range(cnt):
            #print(f'In pog insert: {k}, {new_v[i]}')
            specific_table = self.generate_table(table, cnt, i)
            info_oth = self.group[i].insert(specific_table, k, new_v[i])
            info.memory += info_oth.memory
            info.hash += info_oth.hash
        return info
            
    def delete(self, k):
        info = Info(type='pog.delete')
        
        for i in range(len(self.group)):
            info_oth = self.group[i].delete(k)

            info.memory += info_oth.memory
            info.hash += info_oth.hash
        return info