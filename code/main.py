import othello
import json
import bitarray
from pog_mod import Pog


from common import generate_kv


def test_correct(oth, json_dict, keys):
    cnt = 0
    for i in range(len(json_dict)):
       key = keys[i]
       ans = oth.search(key)

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

n = len(json_dict)
pog = Pog(json_dict)

keys, values = get_keys(json_dict)

'''pg = pog.POG()
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
print(f'Correct is {cnt} of {len(json_dict)}')'''





pog.construct(json_dict)
cnt = test_correct(pog, json_dict, keys)
print(f'Correct is {cnt} of {len(json_dict)}')


# oth.insert(json_dict, "EC:94:9F:FF:A8:37-2051", "1")
# json_dict["EC:94:9F:FF:A8:37-2051"] = '1'


from time import time
start_t = time()
for i in range(10):
    k, v = generate_kv()
    pog.insert(json_dict, k, v)
    pog.delete(k)
    # json_dict[k] = v
end_t = time()

keys, values = get_keys(json_dict)
cnt = test_correct(pog, json_dict, keys)
print(f'Correct is {cnt} of {len(json_dict)}')
print(f"Time spent: {end_t - start_t}")



"""oth.delete(keys[0])
print(f'KEYS[0] = {keys[0]}')
del json_dict[keys[0]]
keys, values = get_keys(json_dict)
cnt = test_correct(oth, json_dict, keys)
print(f'Correct is {cnt} of {len(json_dict)}')"""


