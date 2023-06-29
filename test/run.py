import graphscope as gs
from graphscope.dataset.modern_graph import load_modern_graph

gs.set_option(show_log=True)

# load the modern graph as example.
graph = load_modern_graph()

# Hereafter, you can use the `graph` object to create an `gremlin` query session
g = gs.interactive(graph)
# then `execute` any supported gremlin query.
print(g.graph_url)
q1 = g.execute('g.V().count()')
print(q1.all().result())   # should print [6]
