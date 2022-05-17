from graphscope import nx
G = nx.DiGraph()

edges = [
    (1,3),
    (1,4),
    (2,4),
    (2,5),
    (3,5),
    (1,2)
]
G.add_edges_from(edges)
p = nx.builtin.voterank(G,3)
print(p)