import networkx.algorithms.operators.tests.test_all
import networkx.algorithms.operators.tests.test_binary
import networkx.algorithms.operators.tests.test_product
import networkx.algorithms.operators.tests.test_unary
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.operators.tests.test_all,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.operators.tests.test_binary,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.operators.tests.test_product,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.operators.tests.test_unary,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.usefixtures("graphscope_session")
def test_tensor_product_combinations():
    # basic smoke test, more realistic tests would be useful
    P5 = nx.path_graph(5)
    K3 = nx.complete_graph(3)
    G = nx.tensor_product(P5, K3)
    assert nx.number_of_nodes(G) == 5 * 3

    G = nx.tensor_product(nx.DiGraph(P5), nx.DiGraph(K3))
    assert nx.number_of_nodes(G) == 5 * 3


@pytest.mark.usefixtures("graphscope_session")
def test_lexicographic_product_combinations():
    P5 = nx.path_graph(5)
    K3 = nx.complete_graph(3)
    G = nx.lexicographic_product(P5, K3)
    assert nx.number_of_nodes(G) == 5 * 3


@pytest.mark.usefixtures("graphscope_session")
def test_strong_product_combinations():
    P5 = nx.path_graph(5)
    K3 = nx.complete_graph(3)
    G = nx.strong_product(P5, K3)
    assert nx.number_of_nodes(G) == 5 * 3


@pytest.mark.usefixtures("graphscope_session")
def test_cartesian_product_classic():
    # NB: graphscope.nx does not support grid_2d_graph(which use tuple as node)
    # remove from test
    # test some classic product graphs
    P2 = nx.path_graph(2)
    P3 = nx.path_graph(3)
    # cube = 2-path X 2-path
    G = nx.cartesian_product(P2, P2)
    G = nx.cartesian_product(P2, G)
    assert nx.is_isomorphic(G, nx.cubical_graph())
