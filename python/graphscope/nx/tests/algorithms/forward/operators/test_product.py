import networkx.algorithms.operators.tests.test_product
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.operators.tests.test_product,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


def test_tensor_product_combinations():
    # basic smoke test, more realistic tests would be useful
    P5 = nx.path_graph(5)
    K3 = nx.complete_graph(3)
    G = nx.tensor_product(P5, K3)
    assert nx.number_of_nodes(G) == 5 * 3

    G = nx.tensor_product(nx.DiGraph(P5), nx.DiGraph(K3))
    assert nx.number_of_nodes(G) == 5 * 3


@pytest.mark.skip(reason="not support multigraph")
def test_cartesian_product_multigraph():
    pass


def test_lexicographic_product_combinations():
    P5 = nx.path_graph(5)
    K3 = nx.complete_graph(3)
    G = nx.lexicographic_product(P5, K3)
    assert nx.number_of_nodes(G) == 5 * 3


def test_strong_product_combinations():
    P5 = nx.path_graph(5)
    K3 = nx.complete_graph(3)
    G = nx.strong_product(P5, K3)
    assert nx.number_of_nodes(G) == 5 * 3


@pytest.mark.skip(reason="not support multigraph")
def test_graph_power_raises():
    pass
