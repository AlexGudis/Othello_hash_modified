from collections import defaultdict


class BipartiteGraph:
    """Класс двудольного графа с полезными функциями для Отелло"""

    def __init__(self, size: int) -> None:
        self.adj_list = set()  # индексы соединения вершин вида (u_index, v_index)

        # словарь вида {вершина: [вершины с ней соединенные]}, используется для dfs обхода графа
        self.edges_dict = defaultdict(list)
        self.size = size  # размер доли двудольного графа

    def add_edge(self, u_index: int, v_index: int) -> None:
        """Добавить ребо в структуру графа"""

        self.adj_list.add((u_index, v_index))
        self.edges_dict["U_" + str(u_index)].append("V_" + str(v_index))
        self.edges_dict["V_" + str(v_index)].append("U_" + str(u_index))

    def connected_components(self) -> set | dict | int:
        """Для каждой вершины посчитаем её компоненту связности

        :return
        vertexes - множество всех установленные вершины графа с названиями вида U_<ind> / V_<ind>
        components - словарь: {название вершины : номер компоненты связности}
        num - число компонент связности
        """

        components = {}
        dfs_traversal = list()

        def dfs(vertex: str, component_number: int) -> None:
            """Рекурсивная функция обхода графа в порядке DFS

            :params
            vertexe - название веришины вида U_<ind> / V_<ind>
            component_number - текущий номер компоненты связности
            """

            components[vertex] = component_number

            # Из этой вершины начинаем идти во все, которые с ней соединены
            for u in self.edges_dict[vertex]:
                if u not in components.keys():  # Данной вершине пока что не присвоен номер компоненты связности
                    dfs_traversal.append((vertex, u))
                    dfs(u, component_number)

        num = 0
        vertexes = set()  # Все установленные вершины
        for k, v in self.edges_dict.items():
            vertexes.add(k)
            vertexes |= set(v)

        # print(f"All current vertexes: {vertexes}")

        for v in vertexes:
            # Если вершина не посещена в DFS обходе, то она представляет новую компоненту связности
            if v not in components.keys():
                dfs(v, num + 1)
                num += 1

        # print(f"Connected components count = {num}")
        # print(components)

        return vertexes, components, num, dfs_traversal

    def check_cycle(self) -> bool:
        """Проверка наличия цикла в двудольном графе

        :return
        True, если цикл есть
        False в противном случае

        Используем теорему для компонент связности:
        Цикл есть <=> число ребёр >= числу вершин в данной компоненте
        """
        vertexes, components, num, traversal = self.connected_components()

        Vc = [0] * (num + 1)  # компоненты нумеруются с 1
        for vert in vertexes:
            Vc[components[vert]] += 1

        print(self.adj_list)
        print(self.edges_dict)

        # 2) считаем E_c: каждое ребро лежит в компоненте своей U-вершины
        # чтобы не считать рёбра дважды, смотрим только U вершины
        Ec = [0] * (num + 1)
        for u_index, v_index in self.adj_list:
            u_name = f"U_{u_index}"
            c = components[u_name]
            Ec[c] += 1

        print(Vc)
        print(Ec)

        # Проверка критерия теоремы
        for c in range(1, num + 1):
            if Ec[c] >= Vc[c]:
                return True
        return False


if __name__ == "__main__":
    # Example usage:
    graph = BipartiteGraph(5)

    graph.add_edge(1, 0)
    graph.add_edge(1, 2)
    graph.add_edge(4, 0)
    graph.add_edge(4, 2)
    graph.add_edge(2, 3)

    # graph.display()
    # graph.connected_components()
    print(graph.check_cycle())
