from graph import BipartiteGraph
from hash import FastHash
from math import ceil, log2
from bloom_with_counts import BloomFilterCounter
import bitarray
from dataclasses import dataclass
from typing import Callable
from abstracts import HashAlgorithmBase
import random
import numpy as np

VALUE_DTYPE = np.uint32
INDEX_DTYPE = np.intp
VALUE_MAX = np.iinfo(VALUE_DTYPE).max

# TODO: у всех функций должны быть понятные полные докстринги, все параметры и возвращаемые значения аннотированы
# TODO: сделай код красивым и переходим на pog


class ComponentUF:
    def __init__(self, u_offset: int):
        self.parent: dict[int, int] = {}
        self.size: dict[int, int] = {}

        # какие индексы массива a лежат в этой компоненте
        self.members_u: dict[int, list[int]] = {} 

        # какие индексы массива b лежат в этой компоненте
        # компонента определяется своим корнем
        self.members_v: dict[int, list[int]] = {}
        self.u_offset = u_offset  # = ma (кодировка внутри UF)

    def _make(self, x: int, is_u: bool) -> None:
        if x in self.parent:
            return
        self.parent[x] = x
        self.size[x] = 1
        if is_u:
            self.members_u[x] = [x]
            self.members_v[x] = []
        else:
            self.members_u[x] = []
            self.members_v[x] = [x - self.u_offset]

    def make_u(self, u: int) -> int:
        self._make(u, True)
        return u

    def make_v(self, v: int) -> int:
        x = self.u_offset + v
        self._make(x, False)
        return x

    def find(self, x: int) -> int:
        p = self.parent[x]
        if p != x:
            self.parent[x] = self.find(p)
        return self.parent[x]

    def union(self, a: int, b: int) -> int:
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return ra

        if self.size[ra] < self.size[rb]:
            ra, rb = rb, ra

        self.parent[rb] = ra
        self.size[ra] += self.size[rb]
        self.members_u[ra].extend(self.members_u[rb])
        self.members_v[ra].extend(self.members_v[rb])

        del self.size[rb]
        del self.members_u[rb]
        del self.members_v[rb]
        return ra


@dataclass(slots=True)
class PogQuery:
    """Query structure.

    Хранит только данные, необходимые для lookup:
    - параметры битового разрезания
    - две хеш-функции
    - два битовых массива
    """

    ha: Callable
    hb: Callable
    a: np.ndarray
    b: np.ndarray
    
    def find(self, key: str):
        """Found a value (dest port) for key in MAC-VLAN table"""

        # Проверка, что мы не ищем MAC-VLAN, которых заведомо нет, такие сбрасываем
        # if self.bloom_filter.check_is_not_in_filter(key):
        #     return None

        int_key = FastHash.convert_to_int_key(key)
        i = self.ha(int_key)
        j = self.hb(int_key)
        
        return int(self.a[i] ^ self.b[j])
    


class PogControl(HashAlgorithmBase):
    VALUE_DTYPE = np.uint32

    def __init__(self, table: dict[str, str] = None):
        super().__init__()
        n = len(table)

        # ControlStructure обязана хранить и поддерживать актуальной состояние таблицы
        self.table = dict(table or {})

        # Хеш-функция будет работать как со столбцами, а не индексами в массиве (см миро)
        # self.part_size  = int(n * 1.33)
        self.part_size = 1 << ceil(log2(max(2, int(n * 1.33))))

        # Изначально хеш-функции пустые, определяются на этапе построения
        self.ha = None
        self.hb = None

        # Двудольный граф, изначально пустой
        self.graph = BipartiteGraph()

        # Битовые массивы двудольного графа
        self.a = np.zeros(self.part_size, dtype=self.VALUE_DTYPE)
        self.b = np.zeros(self.part_size, dtype=self.VALUE_DTYPE)
        self.uf = ComponentUF(self.part_size)
        self._uf_dirty = False

        self._query: PogQuery | None = None

        self.construct()

        # Фильтр Блума размера +- равному уникальному числу элементов
        # self.bloom_filter = BloomFilterCounter(self.part_size)

        print(f'Generated Othello structure with array_size = {self.part_size}')

    @staticmethod
    def _to_value_dtype(value: int | str) -> np.uint32:
        ivalue = int(value)
        return np.uint32(ivalue)


    '''def _xor_component(self, root: int, delta: int) -> None:
        if delta == 0:
            return

        for u in self.uf.members_u[root]:
            self.metrics.inc("memory_count")
            self.a[u] ^= delta

        for v in self.uf.members_v[root]:
            self.metrics.inc("memory_count")
            self.b[v] ^= delta'''


    def _xor_component(self, root: int, delta: int) -> None:
        delta_t = self._to_value_dtype(delta)
        if delta_t == 0:
            return

        u_members = self.uf.members_u[root]
        v_members = self.uf.members_v[root]

        if u_members:
            self.metrics.inc("memory_count", len(u_members))
            self.a[np.asarray(u_members, dtype=INDEX_DTYPE)] ^= delta_t

        if v_members:
            self.metrics.inc("memory_count", len(v_members))
            self.b[np.asarray(v_members, dtype=INDEX_DTYPE)] ^= delta_t


    def _rebuild_union_find(self) -> None:
        """Лениво пересобрать Union-Find по текущему графу."""
        self.uf = ComponentUF(self.part_size)

        for (u, v), _t in self.graph.adj_list.items():
            xu = self.uf.make_u(u)
            xv = self.uf.make_v(v)
            self.uf.union(xu, xv)

        self._uf_dirty = False

    def _publish_query(self) -> None:
        if self.ha is None or self.hb is None:
            raise RuntimeError("Hash functions are not initialized")

        self._query = PogQuery(
            ha=self.ha,
            hb=self.hb,
            a=self.a,
            b=self.b,
        )


    def find(self, key: str):
        """Found a value (dest port) for key in MAC-VLAN table"""
        # Делаем "запрос" к query структуре для операции поиска
        # Число вызовов хеш-функций увеливаем на 2, аналогично с памятью
        self.metrics.inc("hash_calls_total")
        self.metrics.inc("hash_calls_total")

        self.metrics.inc("memory_count")
        self.metrics.inc("memory_count")
        return self._query.find(key)
    

    def generate_edges(self) -> tuple[dict[tuple[int, int], int], bool]:
        """Построить граф и UF для текущей пары hash-функций.

        Returns:
            hash_mapping: отображение (u, v) -> значение ребра
            has_cycle: True, если обнаружен цикл или дубль ребра
        """
        hash_mapping: dict[tuple[int, int], int] = {}

        # ВАЖНО: для каждой попытки строим граф и UF заново
        self.graph = BipartiteGraph()
        self.uf = ComponentUF(self.part_size)

        for k, v in self.table.items():
            int_key = FastHash.convert_to_int_key(k)
            u_node = self.ha(int_key)
            v_node = self.hb(int_key)

            self.metrics.inc("hash_calls_total")
            self.metrics.inc("hash_calls_total")

            edge = (u_node, v_node)
            # Ребро дубль это наш цикл
            if edge in self.graph.adj_list:
                return {}, True

            xu = self.uf.make_u(u_node)
            xv = self.uf.make_v(v_node)

            # Если вершины уже в одной компоненте — новое ребро замыкает цикл
            if self.uf.find(xu) == self.uf.find(xv):
                return {}, True

            self.graph.add_edge(u_node, v_node, int(v))
            self.uf.union(xu, xv)
            hash_mapping[edge] = int(v)

        return hash_mapping, False


    def compute_arrays(self, hash_mapping: dict):
        """Заполнить битовые массивы значениями"""

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
            t_k = self._to_value_dtype(hash_mapping[(u_ind, v_ind)])


            self.metrics.inc("memory_count")
            self.metrics.inc("memory_count")
            if u_mark not in computed_vertexes and v_mark not in computed_vertexes:                
                self.a[u_ind] = self.VALUE_DTYPE(0)
                self.b[v_ind] = t_k

                computed_vertexes.add(u_mark)
                computed_vertexes.add(v_mark)

            elif u_mark not in computed_vertexes:
                b_value = self.b[v_ind]
                self.a[u_ind] = b_value ^ t_k
                computed_vertexes.add(u_mark)

            elif v_mark not in computed_vertexes:
                a_value = self.a[u_ind]
                self.b[v_ind] = a_value ^ t_k                
                computed_vertexes.add(v_mark)
            else:
                print("Incorrect traversal")


    def construct(self) -> None:
        """Полностью перестроить Othello-структуру."""

        # При перестроении нужно заново пересчитать размер по текущей таблице (типа как в Кукушке)
        n = len(self.table)
        self.part_size = 1 << ceil(log2(max(2, int(n * 1.33))))
        # Иначе когда приходим из insert, то уже сликшом большая плотность получается и не можем вставить

        while True:
            self.a = np.zeros(self.part_size, dtype=self.VALUE_DTYPE)
            self.b = np.zeros(self.part_size, dtype=self.VALUE_DTYPE)

            seed1 = random.getrandbits(64)
            seed2 = random.getrandbits(64)
            # print(seed1, seed2)
            # seed1 = 1882175618243780441
            # seed2 = 7656401530162172559

            self.ha = FastHash(seed1, self.part_size)
            self.hb = FastHash(seed2, self.part_size)

            self.metrics.inc("hash_calls_total")
            self.metrics.inc("hash_calls_total")

            hash_mapping, has_cycle = self.generate_edges()
            if has_cycle:
                print("Cycle found")
                continue

            break

        self.compute_arrays(hash_mapping)
        self._publish_query()
        self._uf_dirty = False


    def insert(self, key: str, value: str) -> None:
        if self._uf_dirty:
            self._rebuild_union_find()

        t = self._to_value_dtype(value)
        int_key = FastHash.convert_to_int_key(key)

        u = self.ha(int_key)
        v = self.hb(int_key)
        edge = (u, v)

        self.metrics.inc("hash_calls_total")
        self.metrics.inc("hash_calls_total")

        # Коллизия по ребру => цикл/дубликат, полный rebuild
        if edge in self.graph.adj_list:
            self.table[key] = value
            self.construct()
            self.metrics.inc("reconstruction_count")
            return

        xu = self.uf.make_u(u)
        xv = self.uf.make_v(v)

        ru = self.uf.find(xu)
        rv = self.uf.find(xv)

        # Ребро внутри одной компоненты => замкнули цикл
        if ru == rv:
            self.table[key] = value
            self.construct()
            self.metrics.inc("reconstruction_count")
            return

        # Текущая "ошибка" на новом ребре
        current = self.a[u] ^ self.b[v]
        delta = current ^ t
        self.metrics.inc("memory_count")
        self.metrics.inc("memory_count")

        # Перекрашиваем меньшую компоненту
        if self.uf.size[ru] <= self.uf.size[rv]:
            self._xor_component(ru, delta)
        else:
            self._xor_component(rv, delta)

        # Теперь ребро корректно
        self.graph.add_edge(u, v, t)
        self.uf.union(xu, xv)
        self.table[key] = value

   
    def delete(self, key: str) -> bool:
        """Удалить ключ из структуры.

        Возвращает:
            True, если ключ был удалён.
            False, если ключ отсутствовал.
        """
        if key not in self.table:
            return False

        int_key = FastHash.convert_to_int_key(key)
        u_node = self.ha(int_key)
        v_node = self.hb(int_key)

        self.metrics.inc("hash_calls_total")
        self.metrics.inc("hash_calls_total")

        removed = self.graph.remove_edge(u_node, v_node)
        if not removed:
            return False

        del self.table[key]
        self._uf_dirty = True
        return True