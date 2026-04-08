from graph import BipartiteGraph
from hash import FastHash
from math import ceil, log2
from bloom_with_counts import BloomFilterCounter
import bitarray
from dataclasses import dataclass
from typing import Callable
from abstracts import HashAlgorithmBase
import random

# TODO: у всех функций должны быть понятные полные докстринги, все параметры и возвращаемые значения аннотированы
# TODO: сделай код красивым и переходим на pog


@dataclass(slots=True)
class PogQuery:
    """Query structure.

    Хранит только данные, необходимые для lookup:
    - параметры битового разрезания
    - две хеш-функции
    - два битовых массива
    """

    parts_cnt: int
    part_size: int
    ha: Callable
    hb: Callable
    a: bitarray.bitarray
    b: bitarray.bitarray

    def get_value(self, array, hash_number) -> int:
        """Получение значения по хеш-номеру (столбцу)"""

        # value = ""

        # for ind in range(self.parts_cnt):
        #     value += str(array[ind * self.part_size + hash_number])

        # value = int(value, base=2)

        value = 0
        base = hash_number
        for part in range(self.parts_cnt):
            value = (value << 1) | int(array[part * self.part_size + base])
        return value
    
    def find(self, key: str):
        """Found a value (dest port) for key in MAC-VLAN table"""

        # Проверка, что мы не ищем MAC-VLAN, которых заведомо нет, такие сбрасываем
        # if self.bloom_filter.check_is_not_in_filter(key):
        #     return None

        int_key = FastHash.convert_to_int_key(key)
        i = self.ha(int_key)
        j = self.hb(int_key)

        a_value = self.get_value(self.a, i)
        b_value = self.get_value(self.b, j)
        
        return a_value ^ b_value
    


class PogControl(HashAlgorithmBase):

    @staticmethod
    def find_parts_cnt(table: dict):
        """По таблице находим, какая длина должна быть у доли графа, вытягиваем в длину биты"""

        max_elem = max([int(v) for k, v in table.items()])
        return len(bin(max_elem)[2:])

    def __init__(self, table: dict[str, str] = None):
        super().__init__()
        n = len(table)

        # ControlStructure обязана хранить и поддерживать актуальной состояние таблицы
        self.table = dict(table or {})

        # Количество "линейных" массивов в зависимости от максимального номера порта
        self.parts_cnt = PogControl.find_parts_cnt(self.table)

        # Хеш-функция будет работать как со столбцами, а не индексами в массиве (см миро)
        self.part_size  = int(n * 1.33)

        # Размеры битовых массивов для многоклассовой классификации Отелло
        self.ma = self.part_size * self.parts_cnt
        self.mb = self.part_size * self.parts_cnt

        # Изначально хеш-функции пустые, определяются на этапе построения
        self.ha = None
        self.hb = None

        # Сколько бит нужно, чтобы записать номер столбца
        self.hash_size = ceil(log2(self.part_size))

        # Двудольный граф, изначально пустой
        self.graph = BipartiteGraph()

        # Битовые массивы двудольного графа
        self.a = bitarray.bitarray(self.ma)
        self.b = bitarray.bitarray(self.mb)
        self.a.setall(0)
        self.b.setall(0)

        self._query: PogQuery | None = None

        self.construct()

        # Фильтр Блума размера +- равному уникальному числу элементов
        # self.bloom_filter = BloomFilterCounter(self.part_size)

        print(f'Generated Othello structure with ma={
            self.ma}, mb={self.mb}, hash_size={self.hash_size}')
        

    def get_value(self, array, hash_number) -> int:
        """Получение значения по хеш-номеру (столбцу)"""

        # value = ""

        # for ind in range(self.parts_cnt):
        #     value += str(array[ind * self.part_size + hash_number])

        # value = int(value, base=2)

        value = 0
        base = hash_number
        for part in range(self.parts_cnt):
            value = (value << 1) | int(array[part * self.part_size + base])
        return value
    

    def set_value(self, array, hash_number: int, value: int) -> None:
        """Установка значения по хеш-номеру (столбцу)"""

        # indexes = []
        # for p in range(self.parts_cnt):
        #     indexes.append(p * self.part_size + hash_number)

        # inserting_bits = bin(value)[2:]

        # # Нормелизация чила: 11 -> 0011 при 16 портах
        # if len(inserting_bits) != self.parts_cnt:
        #         inserting_bits = '0' * (self.parts_cnt - len(inserting_bits)) + inserting_bits

        # # [(ind, bit)]
        # result = list(zip(indexes, inserting_bits))
        # # print(result)

        # for ind, bit in result:
        #     array[ind] = int(bit)

        for part in range(self.parts_cnt - 1, -1, -1):
            array[part * self.part_size + hash_number] = value & 1
            value >>= 1

    def _publish_query(self) -> None:
        if self.ha is None or self.hb is None:
            raise RuntimeError("Hash functions are not initialized")

        self._query = PogQuery(
            parts_cnt=self.parts_cnt,
            part_size=self.part_size,
            ha=self.ha,
            hb=self.hb,
            a=self.a,
            b=self.b,
        )


    def find(self, key: str):
        """Found a value (dest port) for key in MAC-VLAN table"""
        # Делаем "запрос" к query структуре для операции поиска
        # Число вызовов хеш-функций увеливаем на 2
        self.metrics.inc("hash_calls_total")
        self.metrics.inc("hash_calls_total")
        return self._query.find(key)
    

    def generate_edges(self):
        """Генерация рёбер двудольного графа с классами рёбер"""
        hash_mapping = dict()  # {(u_ind, v_ind): t_k}
        has_cycle = False

        for k, v in self.table.items():

            # Генерируем номера узлов через хеши
            u_node = self.ha(FastHash.convert_to_int_key(k))
            v_node = self.hb(FastHash.convert_to_int_key(k))
            self.metrics.inc("hash_calls_total")
            self.metrics.inc("hash_calls_total")

            if (u_node, v_node) in self.graph.adj_list:
                # Если возникло наложение и дубляж ребра - это цикл
                has_cycle = True
                return hash_mapping, has_cycle

            self.graph.add_edge(u_node, v_node, int(v))
            # self.bloom_filter.add_to_filter(k)

            hash_mapping[(u_node, v_node)] = int(v)

        # print(hash_mapping)
        return hash_mapping, has_cycle

    def compute_arrays(self, hash_mapping: dict):
        computed_vertexes = set()
        traversal = self.graph.connected_components()[3]
        # print(traversal)

        for k, v in traversal:
            u_ind = None
            v_ind = None
            if k.startswith('U'):
                u_ind = int(k.split('_')[1])
                v_ind = int(v.split('_')[1])
            else:
                u_ind = int(v.split('_')[1])
                v_ind = int(k.split('_')[1])

            u_mark = "U_" + str(u_ind)
            v_mark = "V_" + str(v_ind)
            t_k = hash_mapping[(u_ind, v_ind)]

            if u_mark not in computed_vertexes and v_mark not in computed_vertexes:
                self.set_value(self.a, u_ind, 0)
                self.set_value(self.b, v_ind, t_k)

                computed_vertexes.add(u_mark)
                computed_vertexes.add(v_mark)

            elif u_mark not in computed_vertexes:
                b_value = self.get_value(self.b, v_ind)
                self.set_value(self.a, u_ind, b_value ^ t_k)

                computed_vertexes.add(u_mark)

            elif v_mark not in computed_vertexes:
                a_value = self.get_value(self.a, u_ind)
                self.set_value(self.b, v_ind, a_value ^ t_k)
                
                computed_vertexes.add(v_mark)
            else:
                print("Incorrect traversal")

    def construct(self):
        """Create and fill the whole structure of Othello based on MAC-VLAN table"""

        # phase 1
        cycle = True
        hash_mapping = None
        while cycle:
            if hash_mapping:
                # Если значение не None, значит, в цикле уже были и выбрали неверные хеш-функции
                # Необходимо очистить текущее состояние: сбросить граф и битовые массивы
                self.graph = BipartiteGraph()
                self.a.setall(0)
                self.b.setall(0)
                print('Cycle found')

            # self.ha = HashFunction(60, self.hash_size, self.part_size)
            # self.hb = HashFunction(60, self.hash_size, self.part_size)

            self.ha = FastHash(random.getrandbits(64), self.part_size)
            self.hb = FastHash(random.getrandbits(64), self.part_size)
            
            self.metrics.inc("hash_calls_total")
            self.metrics.inc("hash_calls_total")

            hash_mapping, has_cycle = self.generate_edges()
            if has_cycle:
                continue

            cycle = self.graph.check_cycle()

        # phase 2. traversal
        self.compute_arrays(hash_mapping)
        self._publish_query()


    def insert(self, k: str, value: str):
        """Insert a key into Othello structure"""
        "Нужно передавать имеющуюся таблицу на случай невозможности добавить ключ и необходимости перестроения всей структуры"

        # TODO: нужен адекватный dfs обход вершин
        # TODO: нужен корректный поиск компонент связности в графе

        # Генерируем номера узлов через хеши
        u_node = self.ha(FastHash.convert_to_int_key(k))
        v_node = self.hb(FastHash.convert_to_int_key(k))
        self.metrics.inc("hash_calls_total")
        self.metrics.inc("hash_calls_total")

        u_node_sig = "U_" + str(u_node)
        v_node_sig = "V_" + str(v_node)

        if (u_node, v_node) in self.graph.adj_list:
            self.table |= {k: value}
            self.construct()
            return  # Потребовалось перестроение структуры (ребро дубляж)

        old_vertexes = self.graph.get_vertexes()

        self.graph.add_edge(u_node, v_node, int(value))
        # self.bloom_filter.add_to_filter(k)

        if self.graph.check_cycle():
            self.table |= {k: value}
            self.construct()
            print("Reconstruct")
            # Потребовалось перестроение структуры (замкнулся цикл этим ребром)
            return

        # TODO: на уровне идеи
        # В рамках одной компоненты связности выполнить dfs обход в две стороны и понять, в какую выгоднее перекрашивать
        # идти.
        # Предвариетельно проверить через search, что нужна перекраска. Много кейсов, когда это не требуется
        # Например, первый вариант - перекрас dfs по добавленному ребру, а второй - по какому-то другому, соединяющему u v компоненты

        # Ребро успешно добавлено в структуру.
        # 1. Выбираем наименьшую компоненту связности, если соединяются различные
        # 2. Обходим по DFS всю компоненту и перекрашвиаем её

        a_value = self.get_value(self.a, u_node)
        b_value = self.get_value(self.b, v_node)

        if a_value ^ b_value == int(value):
            self.table |= {k: value}
            return  # Вставка прошла успешно, ребро связывает вершины с установелнными корректными индексами в бит массивах

        if u_node_sig not in old_vertexes and v_node_sig not in old_vertexes:
            # Ребро образует новую компоненту связности => установка аналогична этапу construct
            self.set_value(self.a, u_node, 0)
            self.set_value(self.b, v_node, int(value))

            # обновляем таблицу на control structure и публикуем новую query 
            self.table |= {k: value}
            self._publish_query()
            return
        if u_node_sig not in old_vertexes:
            # В текущей компоненте связности появляется вершина u, которая ранее в ней не была => можем вычислить значение
            self.set_value(self.a, u_node, b_value ^ int(value))
            
            # обновляем таблицу на control structure и публикуем новую query 
            self.table |= {k: value}
            self._publish_query()
            return
        if v_node_sig not in old_vertexes:
            # В текущей компоненте связности появляется вершина v, которая ранее в ней не была => можем вычислить значение
            self.set_value(self.b, v_node, a_value ^ int(value))
            
            # обновляем таблицу на control structure и публикуем новую query 
            self.table |= {k: value}
            self._publish_query()
            return

        # Наиболее неприятный случай, когда ребро начало соединять уже установленные вершины и оно некорректно
        # Значит, нужна перекраска новой компоненты
        vertexes, components, num, traversal = self.graph.connected_components()

        # Находим номер полученной компоненты связности
        component_number = components["U_" + str(u_node)]
        visited = set()
        # Начинаем dfs обход только этих вершин

        def dfs(vertex: str, component_number: int) -> None:
            """Рекурсивная функция обхода графа в порядке DFS

            :params
            vertexe - название веришины вида U_<ind> / V_<ind>
            component_number - номер исследуемой компоненты связности
            """

            visited.add(vertex)

            # Из этой вершины начинаем идти во все, которые с ней соединены
            for u in self.graph.edges_dict[vertex]:
                if u not in visited:  # Данную вершину пока что не обошли
                    # print(vertex, u)

                    u_ind = None
                    v_ind = None
                    change = False

                    if u.startswith('U'):
                        u_ind = int(u.split('_')[1])
                        v_ind = int(vertex.split('_')[1])
                    else:
                        u_ind = int(vertex.split('_')[1])
                        v_ind = int(u.split('_')[1])
                        change = True

                    a_value = self.get_value(self.a, u_ind)
                    b_value = self.get_value(self.b, v_ind)

                    # Если бит не соответствет, перекрашиваем
                    if a_value ^ b_value != self.graph.adj_list[(u_ind, v_ind)]:

                        # Что именно перекрашивает зависит от стороны, с которой подошли к вершине
                        # Перекрашиваем ту вершину, которая ещё не в visited
                        if not change:
                            self.set_value(self.a, u_ind, b_value ^ self.graph.adj_list[(u_ind, v_ind)])
                        else:
                            self.set_value(self.b, v_ind, a_value ^ self.graph.adj_list[(u_ind, v_ind)])
                    dfs(u, component_number)

        # Предположительно обход одной компоненты связности не приводит к сильному ускорению работы алгоритма
        # Сейчас обходим полностью всю связывающую компоненту новую и проставляем заново все биты в ней
        # Начинать можно с произвольной новой вершины, dfs обход оставляет в рамках той же компоненты связности
        dfs("U_" + str(u_node), component_number)
        
        # обновляем таблицу на control structure и публикуем новую query 
        self.table |= {k: value}
        self._publish_query()


    def delete(self, key: str):
        """Delete key from Othello structure"""

        # Если ключа нет, то я не могу удалять. Потенциально могу задеть те биты, которые задействованы в вычислении другихх ключей
        #if self.bloom_filter.check_is_not_in_filter(key):
        #    return None

        int_key = FastHash.convert_to_int_key(key)
        u_node = self.ha(int_key)
        v_node = self.hb(int_key)
        self.metrics.inc("hash_calls_total")
        self.metrics.inc("hash_calls_total")

        if self.graph.remove_edge(u_node, v_node):
            del self.table[key]
