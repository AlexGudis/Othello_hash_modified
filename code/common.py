import random


def get_data(filename):
    data = dict()
    with open(filename, "r") as f:
        for el in f.readlines():
            el = el.split()
            data[el[0]] = float(el[1])
    return data


def get_keys(json_dict):
    keys = []
    values = []
    for k, v in json_dict.items():
        keys.append(k)
        values.append(v)
    return keys, values


def generate_mac():
    """Генерирует случайный MAC-адрес."""
    return ':'.join(f"{random.randint(0, 255):02X}" for _ in range(6))


def generate_vlan():
    """Генерирует случайный VLAN ID (от 1 до 4095)."""
    return random.randint(1, 4095)


def generate_kv(port_number=4):
    mac = generate_mac()
    vlan = generate_vlan()
    key = f"{mac}-{vlan}"
    value = str(random.randint(0, port_number))  # Порт
    return key, value