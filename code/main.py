import json
from pog_mod import PogControl
from time import time
from common import generate_kv
from random import randint

def test_correct(oth, json_dict, keys):
    cnt = 0
    for i in range(len(json_dict)):
       key = keys[i]
       ans = oth.find(key)

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
pog = PogControl(json_dict)
keys, values = get_keys(json_dict)
cnt = test_correct(pog, json_dict, keys)
print(f'Correct is {cnt} of {len(json_dict)}')

start_t = time()
for i in range(int(n * 0.1)):
    k, v = generate_kv()
    pog.insert(k, v)
    # pog.delete(k)
    json_dict[k] = v
    # pog.find(keys[randint(1, len(json_dict) - 1)])
end_t = time()

keys, values = get_keys(json_dict)
cnt = test_correct(pog, json_dict, keys)
print(f'Correct is {cnt} of {len(json_dict)}')
print(f"Time spent: {(end_t - start_t) / int(n * 0.1)}")


pog.insert("7C:D9:F6:56:FA:F9-2674", "3")
print(pog.find("7C:D9:F6:56:FA:F9-2674"))


