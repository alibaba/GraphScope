"""Unit tests for the :mod:`graphscope.nx.generators.expanders` module.

"""

import pytest
from networkx import adjacency_matrix

import graphscope.nx as nx
from graphscope.nx import number_of_nodes
from graphscope.nx.generators.expanders import chordal_cycle_graph
from graphscope.nx.generators.expanders import margulis_gabber_galil_graph
from graphscope.nx.generators.expanders import paley_graph


@pytest.mark.usefixtures("graphscope_session")
def test_margulis_gabber_galil_graph():
    for n in 2, 3, 5, 6, 10:
        g = margulis_gabber_galil_graph(n)
        assert number_of_nodes(g) == n * n
        for node in g:
            assert g.degree(node) == 8
            assert len(node) == 2
            for i in node:
                assert int(i) == i
                assert 0 <= i < n

    np = pytest.importorskip("numpy")
    sp = pytest.importorskip("scipy")
    # Eigenvalues are already sorted using the scipy eigvalsh,
    # but the implementation in numpy does not guarantee order.
    w = sorted(sp.linalg.eigvalsh(adjacency_matrix(g).A))
    assert w[-2] < 5 * np.sqrt(2)


@pytest.mark.usefixtures("graphscope_session")
def test_chordal_cycle_graph():
    """Test for the :func:`networkx.chordal_cycle_graph` function."""
    primes = [3, 5, 7, 11]
    for p in primes:
        G = chordal_cycle_graph(p)
        assert len(G) == p


@pytest.mark.usefixtures("graphscope_session")
def test_paley_graph():
    """Test for the :func:`networkx.paley_graph` function."""
    primes = [3, 5, 7, 11, 13]
    for p in primes:
        G = paley_graph(p)
        # G has p nodes
        assert len(G) == p
        # G is (p-1)/2-regular
        in_degrees = {G.in_degree(node) for node in G.nodes}
        out_degrees = {G.out_degree(node) for node in G.nodes}
        assert len(in_degrees) == 1 and in_degrees.pop() == (p - 1) // 2
        assert len(out_degrees) == 1 and out_degrees.pop() == (p - 1) // 2

        # If p = 1 mod 4, -1 is a square mod 4 and therefore the
        # edge in the Paley graph are symmetric.
        if p % 4 == 1:
            for (u, v) in G.edges:
                assert (v, u) in G.edges


@pytest.mark.usefixtures("graphscope_session")
def test_margulis_gabber_galil_graph_badinput():
    pytest.raises(nx.NetworkXError, margulis_gabber_galil_graph, 3, nx.DiGraph())
    pytest.raises(nx.NetworkXError, margulis_gabber_galil_graph, 3, nx.Graph())
