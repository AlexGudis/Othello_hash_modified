import othello
import binascii
import json
import bitarray
from math import log2, ceil
import zlib
import hashlib
import pog


def test_correct(oth, json_dict, keys):
    cnt = 0
    for i in range(len(json_dict)):
       key = keys[i]
       ans = oth.search(key)[0]

       #print(ans,json_dict[key], key)
       if str(json_dict[key]) == str(ans):
              cnt += 1

    return cnt

def get_keys(json_dict):
    keys = []
    values = []
    for k, v in json_dict.items():
       keys.append(k)
       values.append(v)
    return keys, values


with open('mac_vlan_mapping.json', 'r') as JSON:
    json_dict = json.load(JSON)

'''n = len(json_dict)
a = bitarray.bitarray(int(n * 1.33))
b = bitarray.bitarray(int(n * 1.33))
ma = len(a)
mb = len(b)
ha = hashlib.sha3_512
hb = hashlib.sha256
oth = othello.Othello(ma, mb, ha, hb, a, b)'''


keys, values = get_keys(json_dict)

pg = pog.POG()
pg.construct(json_dict)
#pg.search('37:4F:B7:B9:AE:04-791')
cnt = test_correct(pg, json_dict, keys)
print(f'Correct is {cnt} of {len(json_dict)}')

x = input()

pg.insert(json_dict, "EC:94:9F:FG:A8:37-2051", "3")
json_dict["EC:94:9F:FG:A8:37-2051"] = '3'
keys, values = get_keys(json_dict)
cnt = test_correct(pg, json_dict, keys)
print(f'Correct is {cnt} of {len(json_dict)}')

x = input()

pg.delete(keys[0])
del json_dict[keys[0]]
keys, values = get_keys(json_dict)
cnt = test_correct(pg, json_dict, keys)
print(f'Correct is {cnt} of {len(json_dict)}')






'''oth.construct(json_dict)
cnt = test_correct(oth, json_dict, keys)
print(f'Correct is {cnt} of {len(json_dict)}')'''


'''oth.insert(json_dict, "EC:94:9F:FG:A8:37-2051", "1")
json_dict["EC:94:9F:FG:A8:37-2051"] = '1'
keys, values = get_keys(json_dict)
cnt = test_correct(oth, json_dict, keys)
print(f'Correct is {cnt} of {len(json_dict)}')



oth.delete(keys[0])
#print(f'KEYS[0] = {keys[0]}')
del json_dict[keys[0]]
keys, values = get_keys(json_dict)
cnt = test_correct(oth, json_dict, keys)
print(f'Correct is {cnt} of {len(json_dict)}')'''


