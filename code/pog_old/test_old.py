import json
from pog_old import POG_OLD
from time import time
from common import generate_kv

def test_correct(oth, json_dict, keys):
    cnt = 0
    for i in range(len(json_dict)):
       key = keys[i]
       ans = oth.find(key)

       #print(ans,json_dict[key], key)
       if str(json_dict[key]) == str(ans[0]):
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
pog_old = POG_OLD(json_dict)
keys, values = get_keys(json_dict)
cnt = test_correct(pog_old, json_dict, keys)
print(f'Correct is {cnt} of {len(json_dict)}')

start_t = time()
for i in range(int(n * 0.1)):
    k, v = generate_kv()
    pog_old.insert(k, v)
    # pog.delete(k)
    json_dict[k] = v
    # pog.find(keys[randint(1, len(json_dict) - 1)])
end_t = time()

keys, values = get_keys(json_dict)
cnt = test_correct(pog_old, json_dict, keys)
print(f'Correct is {cnt} of {len(json_dict)}')
print(f"Time spent: {(end_t - start_t) / int(n * 0.1)}")


pog_old.insert("7C:D9:F6:56:FA:F9-2674", "1")
print(pog_old.find("7C:D9:F6:56:FA:F9-2674"))

pog_old.insert("7C:D9:F6:56:FA:F9-2674", "0")
print(pog_old.find("7C:D9:F6:56:FA:F9-2674"))