from common import Info
from graph import BipartiteGraph
from hash import HashFunction
from math import ceil, log2
from bloom_with_counts import BloomFilterCounter

# TODO: у всех функций должны быть понятные полные докстринги, все параметры и возвращаемые значения аннотированы
# TODO: сделай код красивым и переходим на pog

class Othello:
    def __init__(self, ma, mb, a, b, ha=None, hb=None):
        self.ma = ma  # The size of bit array a
        self.mb = mb  # The size of bit array b
        self.ha = ha  # Hash function for array a
        self.hb = hb  # Hash function for array a
        # Размер доли двудольного графа (размер битового массива)
        self.part_size = ma
        # Сколько бит нужно, чтобы записать номер ячейки в битовом массиве
        self.hash_size = ceil(log2(self.part_size))
        # Bipartite graph G. It's empty from the start
        self.graph = BipartiteGraph()
        self.a = a  # Bit array a
        self.b = b  # Bit array b

        self.bloom_filter = BloomFilterCounter(ma)

        print(f'Generated Othello structure with ma={
            ma}, mb={mb}, hash_size={self.hash_size}')

    def search(self, key: str):
        """Found a value (dest port) for key in MAC-VLAN table"""

        # Проверка, что мы не ищем MAC-VLAN, которых заведомо нет, такие сбрасываем
        if self.bloom_filter.check_is_not_in_filter(key):
            return None

        i = self.ha(HashFunction.convert_to_int_key(key))
        j = self.hb(HashFunction.convert_to_int_key(key))
        return self.a[i] ^ self.b[j]


    def generate_edges(self, table: dict):
        """Генерация рёбер двудольного графа с классами рёбер"""
        hash_mapping = dict()  # {(u_ind, v_ind): t_k}
        has_cycle = False

        for k, v in table.items():

            # Генерируем номера узлов через хеши
            u_node = self.ha(HashFunction.convert_to_int_key(k))
            v_node = self.hb(HashFunction.convert_to_int_key(k))

            if (u_node, v_node) in self.graph.adj_list:
                # Если возникло наложение и дубляж ребра - это цикл
                has_cycle = True
                return hash_mapping, has_cycle

            self.graph.add_edge(u_node, v_node, int(v))
            self.bloom_filter.add_to_filter(k)

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
                self.a[u_ind] = 0
                self.b[v_ind] = t_k
                computed_vertexes.add(u_mark)
                computed_vertexes.add(v_mark)

            elif u_mark not in computed_vertexes:
                self.a[u_ind] = self.b[v_ind] ^ t_k
                computed_vertexes.add(u_mark)

            elif v_mark not in computed_vertexes:
                self.b[v_ind] = self.a[u_ind] ^ t_k
                computed_vertexes.add(v_mark)
            else:
                print("Incorrect traversal")

    def construct(self, table: dict):
        """Create and fill the whole structure of Othello based on MAC-VLAN table"""

        # phase 1
        cycle = True
        hash_mapping = None
        while cycle:
            if hash_mapping:
                # Если значение не None, значит, в цикле уже были и выбрали неверные хеш-функции
                self.graph = BipartiteGraph()
                print('Cycle found')

            self.ha = HashFunction(60, self.hash_size, self.part_size)
            self.hb = HashFunction(60, self.hash_size, self.part_size)

            hash_mapping, has_cycle = self.generate_edges(table)
            if has_cycle:
                continue

            cycle = self.graph.check_cycle()

        # phase 2. traversal
        self.compute_arrays(hash_mapping)

        print("Hello World")

    def insert(self, table: dict, k: str, value: str):
        """Insert a key into Othello structure"""
        "Нужно передавать имеющуюся таблицу на случай невозможности добавить ключ и необходимости перестроения всей структуры"

        # TODO: нужен адекватный dfs обход вершин
        # TODO: нужен корректный поиск компонент связности в графе

        # Генерируем номера узлов через хеши
        u_node = self.ha(HashFunction.convert_to_int_key(k))
        v_node = self.hb(HashFunction.convert_to_int_key(k))
        u_node_sig = "U_" + str(u_node)
        v_node_sig = "V_" + str(v_node)

        if (u_node, v_node) in self.graph.adj_list:
            self.construct(table | {k: value})
            return  # Потребовалось перестроение структуры (ребро дубляж)
        

        old_vertexes = self.graph.get_vertexes()
        
        self.graph.add_edge(u_node, v_node, int(value))
        self.bloom_filter.add_to_filter(k)

        if self.graph.check_cycle():
            self.construct(table | {k: value})
            print("Reconstruct")
            return # Потребовалось перестроение структуры (замкнулся цикл этим ребром)
        

        # В рамках одной компоненты связности выполнить dfs обход в две стороны и понять, в какую выгоднее перекрашивать
        # идти. 
        # Предвариетельно проверить через search, что нужна перекраска. Много кейсов, когда это не требуется 
        # Например, первый вариант - перекрас dfs по добавленному ребру, а второй - по какому-то другому, соединяющему u v компоненты

        # Ребро успешно добавлено в структуру. 
        # 1. Выбираем наименьшую компоненту связности, если соединяются различные
        # 2. Обходим по DFS всю компоненту и перекрашвиаем её


        if self.a[u_node] ^ self.b[v_node] == value:
            return # Вставка прошла успешно, ребро связывает вершины с установелнными корректными индексами в бит массивах
        

        if u_node_sig not in old_vertexes and v_node_sig not in old_vertexes:
            self.a[u_node] = 0
            self.b[v_node] = int(value)
            return
        elif u_node_sig not in old_vertexes:
            self.a[u_node] = self.b[v_node] ^ int(value)
            return
        if v_node_sig not in old_vertexes:
            self.b[v_node] = self.a[u_node] ^ int(value)
            return
        

        # Наиболее неприятный случай, когда ребро начало соединять уже установленные вершины и оно некорректно
        # Значит, нужно перекраска новой компоненты
        vertexes, components, num, traversal = self.graph.connected_components()


        component_number = components["U_" + str(u_node)] # Находим номер полученной компоненты связности
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

                    # Если бит не соответствет, перекрашиваем
                    if self.a[u_ind] ^ self.b[v_ind] != self.graph.adj_list[(u_ind, v_ind)]:
                        
                        # Что именно перекрашивает зависит от стороны, с которой подошли к вершине
                        # Перекрашиваем ту вершину, которая ещё не в visited
                        if not change:
                            self.a[u_ind] = self.b[v_ind] ^ self.graph.adj_list[(u_ind, v_ind)]
                        else:
                            self.b[v_ind] = self.a[u_ind] ^ self.graph.adj_list[(u_ind, v_ind)]
                    dfs(u, component_number)


        # Предположительно обход одной компоненты связности не приводит к сильному ускорению работы алгоритма
        # Сейчас обходим полностью всю связывающую компоненту новую и проставляем заново все биты в ней
        dfs("U_" + str(u_node), component_number)

    

    def delete(self, key: str):
        """Delete key from Othello structure"""
    
        # Если ключа нет, то я не могу удалять. Потенциально могу задеть те биты, которые задействованы в вычислении другихх ключей
        if self.bloom_filter.check_is_not_in_filter(key):
            return None

        u_node = self.ha(HashFunction.convert_to_int_key(key))
        v_node = self.hb(HashFunction.convert_to_int_key(key))

        # Узлы по классам
        u_node_sig = f"U_{u_node}"
        v_node_sig = f"V_{v_node}"

        self.graph.adj_list.pop((u_node, v_node), None)
        self.graph.edges_dict.pop(u_node_sig, None)
        self.graph.edges_dict.pop(v_node_sig, None)

