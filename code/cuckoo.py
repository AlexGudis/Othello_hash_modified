import hashlib
import random, json
from common import get_keys, generate_kv, Info, test_info, draw
import time

hash_functions = [hashlib.sha1, hashlib.sha224, hashlib.sha256,
                  hashlib.sha384, hashlib.sha3_512, hashlib.sha512, hashlib.sha3_256, hashlib.sha3_384]




load = 35


class Node(object):
    def __init__(self, k, d):
        self.key  = k 
        self.data = d
    def __str__(self):
        return "(" + str(self.key) + ", " + str(self.data) + ")"



class HashTab():
    def __init__(self, size):
        self.array1 = [None] * (size // 2)  # create 2 arrays, both half the length of size
        self.array2 = [None] * (size // 2)
        self.hash1, self.hash2 = random.sample(hash_functions, 2) # random hash func for each array of the cuckoo
        self.keys_cnt = 0                 # total number of nodes 
        self.size = size                  # total size of hash table (both lists in summary)

    # return current number of keys in table    
    def __len__(self): return self.keys_cnt

    # insert and return true, return False if the key/data is already there,
    # grow the table if necessary 
    def insert(self, k, d):
        #self.test_corr(40)
        info = Info('cuko.ins')

        res, find_inf = self.find(k)
        if res != None:  return False, info   # if already there, return False (no duplicates)
        info.hash += find_inf.hash
        info.memory += find_inf.memory

        n = Node(k, d)                           # create a new node with key/data

        # increase size of table if necessary 
        # Так как нам нужно достичь загрузки в 90%, то делать этого мы не будем
        #if self.keys_cnt >= (self.size // 2):
        #    self.growHash()
        
        position1, position2 = self.hashFunc(n.key)  # hash
        info.hash += 2
        # start the loop checking the 1st position in table 1
        pos = position1
        table = self.array1

        '''cycle = False
        pos_1 = set()
        pos_2 = set()

        while not cycle:
            if table[pos] == None:               # if the position in the current table is empty
                table[pos] = n                   # insert the node there and return True
                self.keys_cnt += 1
                return True, info
            info.memory += 1
            
            n, table[pos] = table[pos], n       # else, evict item in pos and insert the item
                                                # then deal with the displaced node.
            info.memory += 1

            if pos == position1:                            # if we're checking the 1st table right now, 
                if pos in pos_1:
                    cycle = True
                position1, position2 = self.hashFunc(n.key) # hash the displaced node,
                pos = position2                             # and check its 2nd position 
                table = self.array2                  # in the 2nd table (next time through loop)
                pos_2.add(pos)
            else:                               
                if pos in pos_2:
                    cycle = True
                position1, position2 = self.hashFunc(n.key) # otherwise, hash the displaced node,
                pos == position1                            # and check the 1st table position. 
                table = self.array1
                pos_1.add(pos)
            info.hash += 2'''



        #       
        for _ in range(5):
            #print('Inserting...')
            
            if table[pos] == None:               # if the position in the current table is empty
                table[pos] = n                   # insert the node there and return True
                self.keys_cnt += 1
                return True, info
            info.memory += 1
            
            n, table[pos] = table[pos], n       # else, evict item in pos and insert the item
                                                # then deal with the displaced node.
            info.memory += 1

            if pos == position1:                            # if we're checking the 1st table right now, 
                position1, position2 = self.hashFunc(n.key) # hash the displaced node,
                pos = position2                             # and check its 2nd position 
                table = self.array2                  # in the 2nd table (next time through loop)
            else:                               
                position1, position2 = self.hashFunc(n.key) # otherwise, hash the displaced node,
                pos == position1                            # and check the 1st table position. 
                table = self.array1
            info.hash += 2


        # This line will never be executed due to infinite loop above
        
        #self.growHash()               # grow and rehash if we make it here. No grow, we want 90% loadfactor  
        
        #print('REHASH')
        info_reh = self.rehash(self.size)                           
        res, info_ins = self.insert(n.key, n.data)      # deal with evicted item
        
        info.hash += info_reh.hash + info_ins.hash
        info.memory +=  info_reh.memory + info_ins.memory

        return True, info

    
    def hashFunc(self, s):
        x = int.from_bytes(self.hash1(s.encode()).digest())      # hash twice 
        y = int.from_bytes(self.hash2(s.encode()).digest())
        
        return x % self.size // 2, y % self.size // 2
    



    # return string representation of both tables
    def __str__(self):
        str1 = "Table 1: [ " + str(self.array1[0]) 
        str2 = " Table 2: [ " + str(self.array2[0]) 
        for i in range(1, self.size):
            str1 += ", " + str(self.array1[i])
        str1 += "]"

        for i in range(1, self.size):
           str2 += ", " + str(self.array2[i]) 
        str2 += "]"
        
        return str1 + str2 
    


    # get new hash functions and reinsert everything 
    def rehash(self, size):        
        temp = HashTab(size)    # create new hash tables
        temp.hash1, temp.hash2 = random.sample(hash_functions, 2)   # get new hash functions
        info = Info('cuko.reh')

        # re-hash each item and insert it into the correct position in the new tables
        for i in range(self.size // 2):
            x = self.array1[i]
            y = self.array2[i]
            info.memory += 2
            if x != None:
                #print(f'INSERTING = {x.key}, {x.data}')
                res, info_ins = temp.insert(x.key, x.data)
                info.memory += info_ins.memory
                info.hash += info_ins.hash
            if y != None:
                #print(f'INSERTING = {y.key}, {y.data}')
                res, info_ins = temp.insert(y.key, y.data)
                info.memory += info_ins.memory
                info.hash += info_ins.hash

        # save new tables S
        # Здесь ведь по идее много обращений к памяти...
        self.array1 = temp.array1
        self.array2 = temp.array2
        self.keys_cnt = temp.keys_cnt
        self.size = temp.size
        self.hash1 = temp.hash1
        self.hash2 = temp.hash2
        
        return info


    # Increase the hash table's size x 2 
    def growHash(self):
        newSize = self.size * 2
        # re-hash each item and insert it into the
        # correct position in the new table
        self.rehash(newSize)
    

    # Return data if there, otherwise return None
    def find(self, k):
        info = Info('cuko.find')

        pos1, pos2 = self.hashFunc(k)               # check both positions the key/data
        info.hash += 2
        x = self.array1[pos1]                       # could be in. return data if found.
        y = self.array2[pos2]  
        info.memory += 2         
        
        if x != None and x.key == k:  return x.data, info
        if y != None and y.key == k:  return y.data, info
    
        # return None if the key can't be found     
        return None, info
    

    # delete the node associated with that key and return True on success
    def delete(self, k):
        info = Info('cuko.del')
        pos1, pos2 = self.hashFunc(k)  
        info.hash += 2
        x = self.array1[pos1]
        y = self.array2[pos2]
        info.memory += 2
        if  x != None and  x.key == k:  self.array1[pos1] = None
        elif y != None and y.key == k:  self.array2[pos2] = None
        else:   return False, info   # the key wasnt found in either possible position
        self.keys_cnt -= 1 
        return True, info
    


def test():
    load = 1000
    missing = 0
    found = 0 

    with open('mac_vlan_mapping.json', 'r') as JSON:
        json_dict = json.load(JSON)

    keys, values = get_keys(json_dict)
    # create a hash table with an initially small number of bukets
    cuko = HashTab(5000)
    inserted = 0
    find_after = 0

    for i in range(load):
        if cuko.insert(keys[i], values[i]):
            inserted += 1

        print(f'Inserted = {inserted}')

        ans, info = cuko.find(keys[i])
        if ans == values[i]:
            find_after += 1
        else:
            print(f'ERROR with k-v pair: {keys[i]}---{values[i]}')
    
    print(f'Correct is {find_after} of {load}')
    print(f'Final size is {cuko.size}')


    """Тестирование среднего числа обращений к памяти и вызовов хеш-функции на операции ВСТАВКИ"""
    """Тестирование среднего числа обращений к памяти и вызовов хеш-функции на операции УДАЛЕНИЕ"""
    insert_memory_cnt = []
    insert_hash_cnt = []
    insert_time = []

    delete_mem_cnt = []
    delete_hash_cnt = []
    delete_time = []
    for _ in range(100):
        new_k, new_v = generate_kv()

        start_t = time.time()
        res, info_ins = cuko.insert(new_k, new_v)
        finish_t = time.time()
        insert_memory_cnt.append(info_ins.memory)
        insert_hash_cnt.append(info_ins.hash)
        insert_time.append(finish_t - start_t)

        start_t = time.time()
        res, info_del = cuko.delete(new_k)
        finish_t = time.time()
        delete_mem_cnt.append(info_del.memory)
        delete_hash_cnt.append(info_del.hash)
        delete_time.append(finish_t - start_t)


    """Тестирование среднего числа обращений к памяти и вызовов хеш-функции на операции ПОИСКА"""
    search_memory_cnt = []
    search_hash_cnt = []
    search_time = []
    cnt = 0
    for i in range(load):
        start_t = time.time()
        ans, info = cuko.find(keys[i])
        finish_t = time.time()
        search_time.append(finish_t - start_t)
        search_memory_cnt.append(info.memory)
        search_hash_cnt.append(info.hash)
        if ans == values[i]:
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


    with open('cuckoo_data', 'w+') as f:
        for k, v in data.items():
            f.writelines(f'{k} {v}\n')


    test_info(avg_insert_mem, avg_delete_mem, avg_search_mem, avg_insert_hash, avg_delete_hash, avg_search_hash, avg_insert_time)

    print(f'Correct is {cnt} of {load}')


def main():
    test()
    
if __name__ == '__main__':
    main()       

