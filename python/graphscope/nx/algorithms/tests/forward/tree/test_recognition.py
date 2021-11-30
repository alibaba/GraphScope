#
# This file is referred and derived from project NetworkX
#
# which has the following license:
#
# Copyright (C) 2004-2020, NetworkX Developers
# Aric Hagberg <hagberg@lanl.gov>
# Dan Schult <dschult@colgate.edu>
# Pieter Swart <swart@lanl.gov>
# All rights reserved.
#
# This file is part of NetworkX.
#
# NetworkX is distributed under a BSD license; see LICENSE.txt for more
# information.
#

import pytest

import graphscope.nx as nx


@pytest.mark.usefixtures("graphscope_session")
class TestTreeRecognition(object):

    graph = nx.Graph

    @classmethod
    def setup_class(cls):

        cls.T1 = cls.graph()

        cls.T2 = cls.graph()
        cls.T2.add_node(1)

        cls.T3 = cls.graph()
        cls.T3.add_nodes_from(range(5))
        edges = [(i, i + 1) for i in range(4)]
        cls.T3.add_edges_from(edges)

        cls.T6 = cls.graph()
        cls.T6.add_nodes_from([6, 7])
        cls.T6.add_edge(6, 7)

        cls.F1 = nx.compose(cls.T6, cls.T3)

        cls.N4 = cls.graph()
        cls.N4.add_node(1)
        cls.N4.add_edge(1, 1)

        cls.N5 = cls.graph()
        cls.N5.add_nodes_from(range(5))

        cls.N6 = cls.graph()
        cls.N6.add_nodes_from(range(3))
        cls.N6.add_edges_from([(0, 1), (1, 2), (2, 0)])

        cls.NF1 = nx.compose(cls.T6, cls.N6)

    def test_null_tree(self):
        with pytest.raises(nx.NetworkXPointlessConcept):
            nx.is_tree(self.graph())

    def test_null_forest(self):
        with pytest.raises(nx.NetworkXPointlessConcept):
            nx.is_forest(self.graph())

    def test_is_tree(self):
        assert nx.is_tree(self.T2)
        assert nx.is_tree(self.T3)

    def test_is_not_tree(self):
        assert not nx.is_tree(self.N4)
        assert not nx.is_tree(self.N5)
        assert not nx.is_tree(self.N6)

    def test_is_forest(self):
        assert nx.is_forest(self.T2)
        assert nx.is_forest(self.T3)
        assert nx.is_forest(self.F1)
        assert nx.is_forest(self.N5)

    def test_is_not_forest(self):
        assert not nx.is_forest(self.N4)
        assert not nx.is_forest(self.N6)
        assert not nx.is_forest(self.NF1)


@pytest.mark.usefixtures("graphscope_session")
class TestDirectedTreeRecognition(TestTreeRecognition):
    graph = nx.DiGraph


@pytest.mark.usefixtures("graphscope_session")
def test_disconnected_graph():
    # https://github.com/networkx/networkx/issues/1144
    G = nx.Graph()
    G.add_edges_from([(0, 1), (1, 2), (2, 0), (3, 4)])
    assert not nx.is_tree(G)

    G = nx.DiGraph()
    G.add_edges_from([(0, 1), (1, 2), (2, 0), (3, 4)])
    assert not nx.is_tree(G)


@pytest.mark.usefixtures("graphscope_session")
def test_dag_nontree():
    G = nx.DiGraph()
    G.add_edges_from([(0, 1), (0, 2), (1, 2)])
    assert not nx.is_tree(G)
    assert nx.is_directed_acyclic_graph(G)


@pytest.mark.usefixtures("graphscope_session")
def test_emptybranch():
    G = nx.DiGraph()
    G.add_nodes_from(range(10))
    assert nx.is_branching(G)
    assert not nx.is_arborescence(G)


@pytest.mark.usefixtures("graphscope_session")
def test_path():
    G = nx.DiGraph()
    nx.add_path(G, range(5))
    assert nx.is_branching(G)
    assert nx.is_arborescence(G)
