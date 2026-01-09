import networkx as nx # TODO: избавиться от этого, нужны самописные решения с понятной реализацией
from networkx.algorithms import bipartite # TODO: Почему это не используется?
import matplotlib.pyplot as plt
import hashlib

from common import Info
from graph import BipartiteGraph
from hash import HashFunction
from math import ceil, log2

hash_functions = [hashlib.sha1, hashlib.sha224, hashlib.sha256,
                  hashlib.sha384, hashlib.sha3_512, hashlib.sha512] # TODO: заменить хотя бы на CRC32

# TODO: реализовать самописную хеш-функцию (см. код Алексея) 
# TODO: добавить алгоритму детерминированности. Все random должны быть с seed, чтобы была воспроизводимость тестов
# Я сильно страдал, пытаясь потом воспроизвести похожие ситуации, на которых всё падало
# TODO: у всех функций должны быть понятные полные докстринги, все параметры и возвращаемые значения аннотированы


class Othello:
    def __init__(self, ma, mb, a, b, ha=None, hb=None):
        self.ma = ma  # The size of bit array a
        self.mb = mb  # The size of bit array b
        self.ha = ha  # Hash function for array a
        self.hb = hb  # Hash function for array a
        self.part_size = ma # Размер доли двудольного графа (размер битового массива)
        self.hash_size = ceil(log2(self.part_size)) # Сколько бит нужно, чтобы записать номер ячейки в битовом массиве
        self.graph = BipartiteGraph(ma)  # Bipartite graph G. It's empty from the start
        self.a = a  # Bit array a
        self.b = b  # Bit array b

        print(f'Generated Othello structure with ma={
            ma}, mb={mb}, hash_size={self.hash_size}')

    def search(self, key):
        """Found a value (dest port) for key in MAC-VLAN table"""
        
        i = self.ha(HashFunction.convert_to_int_key(key))
        j = self.hb(HashFunction.convert_to_int_key(key))
        return self.a[i] ^ self.b[j]

    def check_cycle(self):
        """Checks if any cycle exists in graph g"""
        # TODO: это нужно переделать на своё
        try:
            nx.find_cycle(self.graph)
            return True
        except nx.exception.NetworkXNoCycle:
            return False

    def generate_edges(self, table):
        """Генерация рёбер двудольного графа с классами рёбер"""
        hash_mapping = dict() # {(u_ind, v_ind): t_k}
        has_cycle = False

        for k, v in table.items():
            
            # Генерируем номера узлов через хеши
            left_node = self.ha(HashFunction.convert_to_int_key(k))
            right_node = self.hb(HashFunction.convert_to_int_key(k))

            if (left_node, right_node) in self.graph.adj_list:
                # Если возникло наложение и дубляж ребра - это цикл
                has_cycle = True
                return hash_mapping, has_cycle
            
            self.graph.add_edge(left_node, right_node)

            hash_mapping[(left_node, right_node)] = int(v)
        
        # print(hash_mapping)
        return hash_mapping, has_cycle
            

    def draw_graph(self):
        """Функция рисует граф с раскрашенными рёбрами"""
        left_nodes = [n for n, d in self.graph.nodes(
            data=True) if d["bipartite"] == 0]
        right_nodes = [n for n, d in self.graph.nodes(
            data=True) if d["bipartite"] == 1]
        left_nodes = sorted(left_nodes, reverse=True)
        right_nodes = sorted(right_nodes, reverse=True)
        node_colors = [self.graph.nodes[node]["color"] for node in self.graph.nodes]
        edge_colors = [
            "green" if data['edge_class'] == '1' else "blue" for u,
            v,
            data in self.graph.edges(
                data=True)]

        pos = nx.bipartite_layout(self.graph, left_nodes)

        plt.figure(figsize=(8, 5))
        nx.draw(self.graph, pos, with_labels=True, node_color=node_colors,
                edge_color=edge_colors, width=2, font_color="red")

        plt.show()

    def check_edges_colors(self):
        "Позволяет посмотреть на узлы, ребра, их классы"
        cnt = 0
        for u, v, data in self.graph.edges(data=True):
            print(f"{cnt}: Ребро {u} - {v}, Класс: {data['edge_class']}")
            cnt += 1

    def recolor_both_gray(self, t_k, u, v, i, j):
        self.a[i] = 0
        self.b[j] = t_k
        self.graph.nodes[u]['color'] = "white"
        if t_k:
            self.graph.nodes[v]['color'] = "black"
        else:
            self.graph.nodes[v]['color'] = "white"

    def recolor_not_gray(self, t_k, u, v, i, j):
        if self.graph.nodes[u]['color'] != "gray":  # which means that a[i] is set
            self.b[j] = self.a[i] ^ t_k
            if self.b[j]:
                self.graph.nodes[v]['color'] = 'black'
            else:
                self.graph.nodes[v]['color'] = 'white'
        else:  # which means that b[j] is set
            self.a[i] = self.b[j] ^ t_k
            if self.a[i]:
                self.graph.nodes[u]['color'] = 'black'
            else:
                self.graph.nodes[u]['color'] = 'white'

    def recolor_dfs(self, dfs_edges, info, color_check=False):
        for u, v in dfs_edges:
            u_indexes = u.split('_')
            v_indexes = v.split('_')
            t_k = int(self.graph[u][v]['edge_class'])
            i, j = int(u_indexes[0]), int(v_indexes[0])
            if self.graph.nodes[u]['color'] == "gray" and self.graph.nodes[v]['color'] == "gray":
                #print('Both gray')
                self.recolor_both_gray(t_k, u, v, i, j)

            elif self.graph.nodes[u]['color'] != "gray" or self.graph.nodes[v]['color'] != "gray":
                #print('One of them are not gray')
                self.recolor_not_gray(t_k, u, v, i, j)

            info.memory += 2

            if color_check:
                print(u, v)
                self.draw_graph()
                
        return info

    def recolor(self, info=None):
        if info is None:
            info = Info()

        components = list(nx.connected_components(self.graph))
        all_dfs_edges = []
        for component in components:
            subgraph = self.graph.subgraph(component)
            # Берем любую вершину в компоненте
            start_node = next(iter(component))
            dfs_edges = list(nx.edge_dfs(subgraph, source=start_node))
            all_dfs_edges.extend(dfs_edges)

        all_dfs_edges = [(u, v) if u.endswith("_L") else (v, u)
                         for u, v in all_dfs_edges]
        # print(all_dfs_edges, len(all_dfs_edges))

        info = self.recolor_dfs(all_dfs_edges, info)

        return info
    

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
                self.graph = BipartiteGraph(self.ma)
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



    def insert(self, table, k, v):
        info = Info(type='oth.insert')
        """Insert a key into Othello structure"""
        "Нужно передавать имеющуюся таблицу на случай невозможности добавить ключ и необходимости перестроения всей структуры"

        '''print('Current graph')
        print(self.g.nodes())
        print(self.g.edges())
        self.draw_graph()'''

        # TODO: нужен адекватный dfs обход вершин
        # TODO: нужен корректный поиск компонент связности в графе
        # TODO: уйти от цветов, сильно усложняет реализацию
        # TODO: Можно добавить просто флажок на то, установлен ли какой-то бит у вершины или она условно "серая"

        # Генерируем номера узлов через хеши
        left_node = int.from_bytes(
            self.ha(k.encode()).digest()) % self.hash_size
        right_node = int.from_bytes(
            self.hb(k.encode()).digest()) % self.hash_size
        info.hash += 2

        left_node_sig = f"{left_node}_L"
        right_node_sig = f"{right_node}_R"

        # print(self.g.edges())

        # prself.g[right_node_sig][left_node_sig]
        already_exists = False
        if (left_node_sig, right_node_sig) in self.graph.edges():
            # print('Edge is IN the graph')
            already_exists = True

        '''print(f'We are gonna add ребро {left_node_sig} - {right_node_sig}, класс = {v}')'''
        left_not_in = False
        right_not_in = False

        # 3. Добавляем вершины в граф (если их ещё нет)
        if left_node_sig not in self.graph.nodes():
            left_not_in = True
            self.graph.add_node(left_node_sig, bipartite=0, color="gray")
        if right_node_sig not in self.graph.nodes():
            right_not_in = True
            self.graph.add_node(right_node_sig, bipartite=1, color="gray")

        self.graph.add_edge(left_node_sig, right_node_sig, edge_class=v)

        recolor = False
        if not already_exists:
            if v == '1' and self.graph.nodes[left_node_sig]['color'] == self.graph.nodes[right_node_sig][
                    'color'] or v == '0' and self.graph.nodes[left_node_sig]['color'] != self.graph.nodes[right_node_sig]['color']:
                recolor = True

        '''self.draw_graph()
        print(f'left_not_in = {left_not_in}, right_not_in = {right_not_in}')
        print([self.g.nodes[node] for node in self.g.nodes])'''

        u_indexes = left_node_sig.split('_')
        v_indexes = right_node_sig.split('_')
        t_k = int(v)
        i, j = int(u_indexes[0]), int(v_indexes[0])
        t_k = int(v)

        # case 1 - cycle
        # Или мы добавляем ребро, которое уже есть в графе, значит, возникает
        # цикл длины 2

        '''Да, добавляемое ребро УЖЕ может быть в графе и формально это цикл длины 2, но
        мы не перестраиваем (перекрашиваем) структуру, если корректно установлены биты'''

        if self.check_cycle() or (already_exists):
            print('RECONSTRUCT. Oh shit, make it again...')
            info_check = self.construct(table | {k: v})
            info.hash += info_check.hash
            info.memory += info_check.memory
        elif left_not_in and right_not_in:  # case - просто новая компонента связности в графе
            self.recolor_both_gray(t_k, left_node_sig, right_node_sig, i, j)
            info.memory += 2
            # print('Case both')
        elif left_not_in or right_not_in:  # новая вершина в существующей компоненте связности
            self.recolor_not_gray(t_k, left_node_sig, right_node_sig, i, j)
            info.memory += 2
            # print('Case one')
        elif recolor:  # Новое ребро в существующей компоненте связности и при этом обе вершины уже существуют и при этом класс ребра некорректный

            '''print(f'Oh man, recolor it...')
            print(self.g[right_node_sig][left_node_sig])'''

            dfs_edges = list(nx.dfs_edges(self.graph, source=left_node_sig))

            def is_L(node):
                return str(node).endswith('_L')

            def sort_edge(u, v):
                return (u, v) if is_L(u) else (v, u)

            dfs_sorted_edges = [sort_edge(u, v) for u, v in dfs_edges]
            # print([(ll,rr,self.g[ll][rr]['edge_class']) for ll, rr in dfs_sorted_edges])

            already_seen = set()  # Если вершины ещё не было, то её и перекрашиваю
            for u, v in dfs_sorted_edges:
                u_indexes = u.split('_')
                v_indexes = v.split('_')
                t_k = int(self.graph[u][v]['edge_class'])
                i, j = int(u_indexes[0]), int(v_indexes[0])

                '''
                print()
                print(self.a[i], self.b[j])
                print(f'u_color = {self.g.nodes[u]['color']}, v_color = {self.g.nodes[v]['color']}, t_k = {t_k}')
                '''

                if (self.graph.nodes[u]['color'] != self.graph.nodes[v]['color'] and t_k == 0) or (
                        self.graph.nodes[u]['color'] == self.graph.nodes[v]['color'] and t_k == 1):
                    # Нужно перекрашивать это ребро
                    # print('Color them')
                    if u not in already_seen:
                        self.a[i] = self.b[j] ^ t_k
                        # already_seen.append(u)
                        if self.a[i]:
                            self.graph.nodes[u]['color'] = 'black'
                        else:
                            self.graph.nodes[u]['color'] = 'white'
                        info.memory += 2
                    elif v not in already_seen:
                        self.b[j] = self.a[i] ^ t_k
                        # already_seen.append(v)
                        if self.b[j]:
                            self.graph.nodes[v]['color'] = 'black'
                        else:
                            self.graph.nodes[v]['color'] = 'white'
                        info.memory += 3
                already_seen.add(u)
                already_seen.add(v)

                '''
                print(u, v)
                print(self.a[i], self.b[j])
                print(already_seen)
                self.draw_graph()
                '''

        else:
            # print('Nothing to do')
            # print(f'Current class = {self.g[right_node_sig][left_node_sig]} /// and wanted class = {t_k}')
            pass

        '''self.draw_graph()'''

        return info


    # TODO: Вероятно, избавиться от этого за ненадобностью и отсутствием use-cases
    def addX(self, k):
        """Input key into X"""
        pass

    def addY(self, k):
        """Input key into Y"""
        pass

    def alter(self, k):
        """Change key value place"""
        pass

    def delete(self, k):
        """Delete key from Othello structure"""
        info = Info('oth.delete')

        '''self.draw_graph()'''

        # Генерируем номера узлов через хеши
        left_node = int.from_bytes(
            self.ha(k.encode()).digest()) % self.hash_size
        right_node = int.from_bytes(
            self.hb(k.encode()).digest()) % self.hash_size
        info.hash += 2
        info.memory += 0

        # Узлы без классов
        # print(self.g.edges)
        # print(k)
        left_node_sig = f"{left_node}_L"
        right_node_sig = f"{right_node}_R"
        # print(f'DELETE {left_node_sig} {right_node_sig} with key {k}')

        self.graph.remove_edge(left_node_sig, right_node_sig)

        '''self.draw_graph()'''

        return info

