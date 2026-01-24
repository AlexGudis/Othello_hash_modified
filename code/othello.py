from common import Info
from graph import BipartiteGraph
from hash import HashFunction
from math import ceil, log2

# TODO: у всех функций должны быть понятные полные докстринги, все параметры и возвращаемые значения аннотированы


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

        print(f'Generated Othello structure with ma={
            ma}, mb={mb}, hash_size={self.hash_size}')

    def search(self, key):
        """Found a value (dest port) for key in MAC-VLAN table"""

        i = self.ha(HashFunction.convert_to_int_key(key))
        j = self.hb(HashFunction.convert_to_int_key(key))
        return self.a[i] ^ self.b[j]


    def generate_edges(self, table):
        """Генерация рёбер двудольного графа с классами рёбер"""
        hash_mapping = dict()  # {(u_ind, v_ind): t_k}
        has_cycle = False

        for k, v in table.items():

            # Генерируем номера узлов через хеши
            left_node = self.ha(HashFunction.convert_to_int_key(k))
            right_node = self.hb(HashFunction.convert_to_int_key(k))

            if (left_node, right_node) in self.graph.adj_list:
                # Если возникло наложение и дубляж ребра - это цикл
                has_cycle = True
                return hash_mapping, has_cycle

            self.graph.add_edge(left_node, right_node, int(v))

            hash_mapping[(left_node, right_node)] = int(v)

        # print(hash_mapping)
        return hash_mapping, has_cycle

    def compute_arrays(self, hash_mapping):
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

    def construct(self, table):
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

    def insert(self, table, k, value):
        """Insert a key into Othello structure"""
        "Нужно передавать имеющуюся таблицу на случай невозможности добавить ключ и необходимости перестроения всей структуры"

        # TODO: нужен адекватный dfs обход вершин
        # TODO: нужен корректный поиск компонент связности в графе

        # Генерируем номера узлов через хеши
        left_node = self.ha(HashFunction.convert_to_int_key(k))
        right_node = self.hb(HashFunction.convert_to_int_key(k))

        if (left_node, right_node) in self.graph.adj_list:
            self.construct(table | {k: value})
            return  # Потребовалось перестроение структуры (ребро дубляж)
        
        self.graph.add_edge(left_node, right_node, int(value))

        if self.graph.check_cycle():
            self.construct(table | {k: value})
            return # Потребовалось перестроение структуры (замкнулся цикл этим ребром)
        

        # В рамках одной компоненты связности выполнить dfs обход в две стороны и понять, в какую выгоднее перекрашивать
        # идти. 
        # Предвариетельно проверить через search, что нужна перекраска. Много кейсов, когда это не требуется 
        # Например, первый вариант - перекрас dfs по добавленному ребру, а второй - по какому-то другому, соединяющему u v компоненты

        # Ребро успешно добавлено в структуру. 
        # 1. Выбираем наименьшую компоненту связности, если соединяются различные
        # 2. Обходим по DFS всю компоненту и перекрашвиаем её


        if self.a[left_node] ^ self.b[right_node] == value:
            return # Вставка прошла успешно, ребро связывает вершины с установелнными корректными индексами в бит массивах
        
        
        vertexes, components, num, traversal = self.graph.connected_components()
        component_number = components["U_" + str(left_node)] # Находим номер полученной компоненты связности
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
                if u not in visited and components[u] == component_number:  # Данную вершину пока что не обошли
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

        for v in vertexes:
            # Если вершина не посещена в DFS обходе, то она представляет новую компоненту связности
            if v not in visited and components[v] == component_number:
                # print(v)
                dfs(v, component_number)

    

    def delete(self, k):
        """Delete key from Othello structure"""
        
        # Вот здесь нужна проверка, что ключа ТОЧНО нет
        # Если его точно нет, то и вырезать нельзя, так как из-за коллизии хешей
        # можем вырезать тот ключ, который не собирался быть удаленным

        left_node = self.ha(HashFunction.convert_to_int_key(k))
        right_node = self.hb(HashFunction.convert_to_int_key(k))

        # Узлы по классам
        left_node_sig = f"U_{left_node}"
        right_node_sig = f"V_{right_node}"

        self.graph.adj_list.pop((left_node, right_node), None)
        self.graph.edges_dict.pop(left_node_sig, None)
        self.graph.edges_dict.pop(right_node_sig, None)

