import math
from bitarray import bitarray
from hash import HashFunction
from math import ceil, log2

class BloomFilterCounter(object):

    def __init__(self, expected_elements):

        #Предположительно число элементов Отелло максимум увеличивается в 3 раза от исходного размера 
        # TODO: для достижения производительности нужно будет подумать над размерами.
        # Сейчас выигрываем по функциональности, но начинаем проигрывать по памяти
        self.size = expected_elements * 3
        self.expected_elements = expected_elements

        self.bloom_filter = bitarray(self.size)
        self.bloom_filter.setall(0)
        self.counter = [0] * self.size


        # Считается оптимальным кол-вом хеш-функций
        self.number_hash_functions = round((self.size / self.expected_elements) * math.log(2)) 
        print("Optimal hash functions count = ", self.number_hash_functions)
        self.hash_fuctions = [HashFunction(60, ceil(log2(self.size)), self.size) for _ in range(self.number_hash_functions)]

    def add_to_filter(self, key):
        for i in range(self.number_hash_functions):
            ind = self.hash_fuctions[i](HashFunction.convert_to_int_key(key))
            self.bloom_filter[ind] = 1
            self.counter[ind] += 1


    def check_is_not_in_filter(self, key):
        # элемент НЕ входит в множество

        for i in range(self.number_hash_functions):
            ind = self.hash_fuctions[i](HashFunction.convert_to_int_key(key))
            if self.bloom_filter[ind] == 0:
                return True
        return False
    
    def delete(self, key):
        for i in range(self.number_hash_functions):
            ind = self.hash_fuctions[i](HashFunction.convert_to_int_key(key))
            self.counter[ind] -= 1

            if not self.counter[ind]:
                # Если число счётчиков стало нулевым, то больше нет хеш-функции, попадающей в этот бит
                self.bloom_filter[ind] = 0

        


if __name__ == "__main__":
    bloom_filter = BloomFilterCounter(1000000)
    mac_vlan = "F6:02:69:78:88:E0-118"
    bloom_filter.add_to_filter(mac_vlan + str(0))

    for i in range(10):
        if not bloom_filter.check_is_not_in_filter(mac_vlan + str(i)):
            print("Before delete")
            print(mac_vlan+str(i))


    bloom_filter.delete(mac_vlan + str(0))

    for i in range(10):
        if not bloom_filter.check_is_not_in_filter(mac_vlan + str(i)):
            print("After delete")
            print(mac_vlan+str(i))