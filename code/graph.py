from collections import defaultdict

class GraphNode:
    def __init__(self):
        self.is_set = False # флаг установки значения вершины


class BipartiteGraph:
    def __init__(self, size):

        self.U = [GraphNode() for _ in range(size)]
        self.V = [GraphNode() for _ in range(size)]
        self.adj_list = set()  # индексы соединения вершин
        self.g = defaultdict(list)
        self.size = size

    def add_edge(self, u_index, v_index):
        self.U[u_index].is_set = True
        self.V[v_index].is_set = True
        self.adj_list.add((u_index, v_index))
        self.g["U_" + str(u_index)].append("V_" + str(v_index))
        self.g["V_" + str(v_index)].append("U_" + str(u_index))


    def connected_components(self):
        """Для каждой вершины вернём её компоненту связности"""
        components = {}

        def dfs(vertex, component_number):
            components[vertex] = component_number

            for u in self.g[vertex]:
                if u not in components.keys(): # Данной вершине пока что не присвоен номер компоненты связности
                    dfs(u, component_number)
        
        num = 0
        vertexes = set() # Все участвующие вершины
        for k, v in self.g.items():
            vertexes.add(k)
            vertexes.union(set(v))
        
        print(f"All current vertexes: {vertexes}")

        for v in vertexes:
            if v not in components.keys():
                dfs(v, num + 1)
                num += 1

        print(f"Connected components count = {num}")
        print(components)
        



    def check_cycle(self):
        


    def display(self):
        print([(i, self.U[i].is_set) for i in range(len(self.U))])
        print([(j, self.V[j].is_set) for j in range(len(self.V))])
        print("Adjacency List:")
        for el in self.adj_list:
            print(f"{el}")
        print()
        print(self.g)


if __name__ == "__main__":
    # Example usage:
    graph = BipartiteGraph(4)

    graph.add_edge(0, 3)
    graph.add_edge(0, 2)
    graph.add_edge(2, 1)

    # graph.display()
    graph.connected_components()
