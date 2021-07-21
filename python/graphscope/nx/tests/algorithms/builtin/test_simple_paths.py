#
# This file test_bfs.py is referred and derived from project NetworkX
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

from graphscope import nx
from graphscope.nx.tests.utils import almost_equal


@pytest.mark.usefixtures("graphscope_session")
class TestIsSimplePath:
    """Unit tests for the
    :func:`networkx.algorithms.simple_paths.is_simple_path` function.
    """

    def test_empty_list(self):
        """Tests that the empty list is not a valid path, since there
        should be a one-to-one correspondence between paths as lists of
        nodes and paths as lists of edges.
        """
        G = nx.trivial_graph()
        assert not nx.builtin.is_simple_path(G, [])

    def test_trivial_path(self):
        """Tests that the trivial path, a path of length one, is
        considered a simple path in a graph.
        """
        G = nx.trivial_graph()
        assert nx.builtin.is_simple_path(G, [0])

    def test_trivial_nonpath(self):
        """Tests that a list whose sole element is an object not in the
        graph is not considered a simple path.
        """
        G = nx.trivial_graph()
        assert not nx.builtin.is_simple_path(G, ["not a node"])

    def test_simple_path(self):
        G = nx.path_graph(2)
        assert nx.builtin.is_simple_path(G, [0, 1])

    def test_non_simple_path(self):
        G = nx.path_graph(2)
        assert not nx.builtin.is_simple_path(G, [0, 1, 0])

    def test_cycle(self):
        G = nx.cycle_graph(3)
        assert not nx.builtin.is_simple_path(G, [0, 1, 2, 0])

    def test_missing_node(self):
        G = nx.path_graph(2)
        assert not nx.builtin.is_simple_path(G, [0, 2])

    def test_directed_path(self):
        G = nx.DiGraph([(0, 1), (1, 2)])
        assert nx.builtin.is_simple_path(G, [0, 1, 2])

    def test_directed_non_path(self):
        G = nx.DiGraph([(0, 1), (1, 2)])
        assert not nx.builtin.is_simple_path(G, [2, 1, 0])

    def test_directed_cycle(self):
        G = nx.DiGraph([(0, 1), (1, 2), (2, 0)])
        assert not nx.builtin.is_simple_path(G, [0, 1, 2, 0])

    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph(self):
        G = nx.MultiGraph([(0, 1), (0, 1)])
        assert nx.builtin.is_simple_path(G, [0, 1])

    @pytest.mark.skip(reason="not support multidigraph")
    def test_multidigraph(self):
        G = nx.MultiDiGraph([(0, 1), (0, 1), (1, 0), (1, 0)])
        assert nx.builtin.is_simple_path(G, [0, 1])

    def test_not_list(self):
        G = nx.path_graph(2)
        with pytest.raises(ValueError, match="input nodes is not a list object!"):
            ctx = nx.builtin.is_simple_path(G, 1)
