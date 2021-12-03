import networkx.algorithms.tests.test_triads
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.tests.test_triads,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from collections import defaultdict
from itertools import permutations
from random import sample

import graphscope.nx as nx


# FIXME(@weibin): forward is_triad does not replace networkx with graphscope.nx correctly.
def is_triad(G):
    """Returns True if the graph G is a triad, else False.

    Parameters
    ----------
    G : graph
       A NetworkX Graph

    Returns
    -------
    istriad : boolean
       Whether G is a valid triad
    """
    if isinstance(G, nx.Graph):
        if G.order() == 3 and nx.is_directed(G):
            if not any((n, n) in G.edges() for n in G.nodes()):
                return True
    return False


# FIXME(@weibin): forward is_triad does not replace networkx with graphscope.nx correctly.
def triad_type(G):
    if not is_triad(G):
        raise nx.NetworkXAlgorithmError("G is not a triad (order-3 DiGraph)")
    num_edges = len(G.edges())
    if num_edges == 0:
        return "003"
    if num_edges == 1:
        return "012"
    if num_edges == 2:
        e1, e2 = G.edges()
        if set(e1) == set(e2):
            return "102"
        if e1[0] == e2[0]:
            return "021D"
        if e1[1] == e2[1]:
            return "021U"
        if e1[1] == e2[0] or e2[1] == e1[0]:
            return "021C"
    if num_edges == 3:
        for (e1, e2, e3) in permutations(G.edges(), 3):
            if set(e1) == set(e2):
                if e3[0] in e1:
                    return "111U"
                # e3[1] in e1:
                return "111D"
            if set(e1).symmetric_difference(set(e2)) == set(e3):
                if {e1[0], e2[0], e3[0]} == {e1[0], e2[0], e3[0]} == set(G.nodes()):
                    return "030C"
                # e3 == (e1[0], e2[1]) and e2 == (e1[1], e3[1]):
                return "030T"
    if num_edges == 4:
        for (e1, e2, e3, e4) in permutations(G.edges(), 4):
            if set(e1) == set(e2):
                # identify pair of symmetric edges (which necessarily exists)
                if set(e3) == set(e4):
                    return "201"
                if {e3[0]} == {e4[0]} == set(e3).intersection(set(e4)):
                    return "120D"
                if {e3[1]} == {e4[1]} == set(e3).intersection(set(e4)):
                    return "120U"
                if e3[1] == e4[0]:
                    return "120C"
    if num_edges == 5:
        return "210"
    if num_edges == 6:
        return "300"


def triads_by_type(G):
    """Returns a list of all triads for each triad type in a directed graph.

    Parameters
    ----------
    G : digraph
       A NetworkX DiGraph

    Returns
    -------
    tri_by_type : dict
       Dictionary with triad types as keys and lists of triads as values.
    """
    # num_triads = o * (o - 1) * (o - 2) // 6
    # if num_triads > TRIAD_LIMIT: print(WARNING)
    all_tri = nx.all_triads(G)
    tri_by_type = defaultdict(list)
    for triad in all_tri:
        name = triad_type(triad)
        tri_by_type[name].append(triad)
    return tri_by_type


@pytest.mark.usefixtures("graphscope_session")
def test_is_triad():
    """Tests the is_triad function"""
    G = nx.karate_club_graph()
    G = G.to_directed()
    for i in range(100):
        nodes = sample(G.nodes(), 3)
        G2 = G.subgraph(nodes)
        assert is_triad(G2)


@pytest.mark.usefixtures("graphscope_session")
def test_triad_type():
    """Tests the triad_type function."""
    # 0 edges (1 type)
    G = nx.DiGraph({0: [], 1: [], 2: []})
    assert triad_type(G) == "003"
    # 1 edge (1 type)
    G = nx.DiGraph({0: [1], 1: [], 2: []})
    assert triad_type(G) == "012"
    # 2 edges (4 types)
    G = nx.DiGraph([(0, 1), (0, 2)])
    assert triad_type(G) == "021D"
    G = nx.DiGraph({0: [1], 1: [0], 2: []})
    assert triad_type(G) == "102"
    G = nx.DiGraph([(0, 1), (2, 1)])
    assert triad_type(G) == "021U"
    G = nx.DiGraph([(0, 1), (1, 2)])
    assert triad_type(G) == "021C"
    # 3 edges (4 types)
    G = nx.DiGraph([(0, 1), (1, 0), (2, 1)])
    assert triad_type(G) == "111D"
    G = nx.DiGraph([(0, 1), (1, 0), (1, 2)])
    assert triad_type(G) == "111U"
    G = nx.DiGraph([(0, 1), (1, 2), (0, 2)])
    assert triad_type(G) == "030T"
    G = nx.DiGraph([(0, 1), (1, 2), (2, 0)])
    assert triad_type(G) == "030C"
    # 4 edges (4 types)
    G = nx.DiGraph([(0, 1), (1, 0), (2, 0), (0, 2)])
    assert triad_type(G) == "201"
    G = nx.DiGraph([(0, 1), (1, 0), (2, 0), (2, 1)])
    assert triad_type(G) == "120D"
    G = nx.DiGraph([(0, 1), (1, 0), (0, 2), (1, 2)])
    assert triad_type(G) == "120U"
    G = nx.DiGraph([(0, 1), (1, 0), (0, 2), (2, 1)])
    assert triad_type(G) == "120C"
    # 5 edges (1 type)
    G = nx.DiGraph([(0, 1), (1, 0), (2, 1), (1, 2), (0, 2)])
    assert triad_type(G) == "210"
    # 6 edges (1 type)
    G = nx.DiGraph([(0, 1), (1, 0), (1, 2), (2, 1), (0, 2), (2, 0)])
    assert triad_type(G) == "300"


@pytest.mark.usefixtures("graphscope_session")
def test_triads_by_type():
    """Tests the all_triplets function."""
    G = nx.DiGraph()
    G.add_edges_from(["01", "02", "03", "04", "05", "12", "16", "51", "56", "65"])
    all_triads = nx.all_triads(G)
    expected = defaultdict(list)
    for triad in all_triads:
        name = triad_type(triad)
        expected[name].append(triad)
    actual = triads_by_type(G)
    assert set(actual.keys()) == set(expected.keys())
    for tri_type, actual_Gs in actual.items():
        expected_Gs = expected[tri_type]
        for a in actual_Gs:
            assert any(nx.is_isomorphic(a, e) for e in expected_Gs)


@pytest.mark.usefixtures("graphscope_session")
def test_random_triad():
    """Tests the random_triad function"""
    G = nx.karate_club_graph()
    G = G.to_directed()
    for i in range(100):
        assert is_triad(nx.random_triad(G))