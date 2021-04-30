#import pytest

#import networkx.algorithms.connectivity.tests.test_edge_augmentation

#from graphscope.nx.utils.compat import import_as_graphscope_nx

#import_as_graphscope_nx(
#    networkx.algorithms.connectivity.tests.test_edge_augmentation,
#    decorators=pytest.mark.usefixtures("graphscope_session")
#)

#def test_is_locally_k_edge_connected_exceptions():
#    pytest.raises(nx.NetworkXNotImplemented,
#                  is_k_edge_connected,
#                  nx.DiGraph(), k=0)
#    pytest.raises(ValueError, is_k_edge_connected,
#                  nx.Graph(), k=0)

#def test_is_k_edge_connected_exceptions():
#    pytest.raises(nx.NetworkXNotImplemented,
#                  is_locally_k_edge_connected,
#                  nx.DiGraph(), 1, 2, k=0)
#    pytest.raises(ValueError,
#                  is_locally_k_edge_connected,
#                  nx.Graph(), 1, 2, k=0)
