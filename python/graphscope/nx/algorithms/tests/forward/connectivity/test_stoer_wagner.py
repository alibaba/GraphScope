import pytest

#import networkx.algorithms.connectivity.tests.test_stoer_wagner

#from graphscope.nx.utils.compat import import_as_graphscope_nx

#import_as_graphscope_nx(
#    networkx.algorithms.connectivity.tests.test_stoer_wagner,
#    decorators=pytest.mark.usefixtures("graphscope_session")
#)


@pytest.mark.skip(reason="not support multigraph")
def test_exceptions():
    G = nx.Graph()
    pytest.raises(nx.NetworkXError, nx.stoer_wagner, G)
    G.add_node(1)
    pytest.raises(nx.NetworkXError, nx.stoer_wagner, G)
    G.add_node(2)
    pytest.raises(nx.NetworkXError, nx.stoer_wagner, G)
    G.add_edge(1, 2, weight=-2)
    pytest.raises(nx.NetworkXError, nx.stoer_wagner, G)
    G = nx.DiGraph()
    pytest.raises(nx.NetworkXNotImplemented, nx.stoer_wagner, G)
