import networkx.algorithms.tests.test_hybrid
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.tests.test_hybrid,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

@pytest.mark.skip(reason="graphscope.nx does not support grid_2d_graph.")
def test_2d_grid_graph():
    # FC article claims 2d grid graph of size n is (3,3)-connected
    # and (5,9)-connected, but I don't think it is (5,9)-connected
    G = nx.grid_2d_graph(8, 8, periodic=True)
    assert nx.is_kl_connected(G, 3, 3)
    assert not nx.is_kl_connected(G, 5, 9)
    (H, graphOK) = nx.kl_connected_subgraph(G, 5, 9, same_as_graph=True)
    assert not graphOK
