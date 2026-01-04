import json
import random
import argparse
from common import generate_mac, generate_vlan

def generate_json(filename="output.json", num_entries=10):
    """Генерирует JSON-файл с парами MAC-VLAN и случайными значениями портов."""
    data = {}
    for _ in range(num_entries):
        mac = generate_mac()
        vlan = generate_vlan()
        key = f"{mac}-{vlan}"
        value = str(random.randint(0, 5))  # Порт
        data[key] = value
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
    
    print(f"Файл {filename} сгенерирован.")


parser = argparse.ArgumentParser()
parser.add_argument(
    "-n",
    type=int,
    help="Number of lines in MAC-VLAN table",
    dest="row_count",
    default=10,
)
args = parser.parse_args()

generate_json("mac_vlan_mapping.json", num_entries=args.row_count)