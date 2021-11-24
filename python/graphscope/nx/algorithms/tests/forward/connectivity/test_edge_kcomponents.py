import pytest

#import networkx.algorithms.connectivity.tests.test_edge_kcomponents

#from graphscope.nx.utils.compat import import_as_graphscope_nx

#import_as_graphscope_nx(
#    networkx.algorithms.connectivity.tests.test_edge_kcomponents,
#    decorators=pytest.mark.usefixtures("graphscope_session")
#)


@pytest.mark.skip(reason="not support multigraph")
def test_not_implemented():
    G = nx.MultiGraph()
    pytest.raises(nx.NetworkXNotImplemented, EdgeComponentAuxGraph.construct, G)
    pytest.raises(nx.NetworkXNotImplemented, nx.k_edge_components, G, k=2)
    pytest.raises(nx.NetworkXNotImplemented, nx.k_edge_subgraphs, G, k=2)
    pytest.raises(nx.NetworkXNotImplemented, bridge_components, G)
    pytest.raises(nx.NetworkXNotImplemented, bridge_components, nx.DiGraph())
