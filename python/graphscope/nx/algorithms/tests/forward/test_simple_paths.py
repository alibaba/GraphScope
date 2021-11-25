import networkx as nxa
import networkx.algorithms.tests.test_simple_paths
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.tests.test_simple_paths,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.usefixtures("graphscope_session")
def test_shortest_simple_paths():
    # NB: graphscope.nx does not support grid_2d_graph, we convert from networkx
    H = cnlti(nxa.grid_2d_graph(4, 4), first_label=1, ordering="sorted")
    G = nx.Graph(H)
    paths = nx.shortest_simple_paths(G, 1, 12)
    assert next(paths) == [1, 2, 3, 4, 8, 12]
    assert next(paths) == [1, 5, 6, 7, 8, 12]
    assert [len(path) for path in nx.shortest_simple_paths(G, 1, 12)] == sorted(
        [len(path) for path in nx.all_simple_paths(G, 1, 12)]
    )

@pytest.mark.usefixtures("graphscope_session")
@pytest.mark.skipif(nxa.__version__ < "2.5", reason="netowrkx2.4 does not support weight funtion.")
def test_shortest_simple_paths_directed_with_weight_fucntion():
    def cost(u, v, x):
        return 1

    # NB: graphscope.nx does not support grid_2d_graph, we convert from networkx
    H = cnlti(nxa.grid_2d_graph(4, 4), first_label=1, ordering="sorted")
    G = nx.Graph(H)
    paths = nx.shortest_simple_paths(G, 1, 12)
    assert next(paths) == [1, 2, 3, 4, 8, 12]
    assert next(paths) == [1, 5, 6, 7, 8, 12]
    assert [
        len(path) for path in nx.shortest_simple_paths(G, 1, 12, weight=1)
    ] == sorted([len(path) for path in nx.all_simple_paths(G, 1, 12)])
