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
import os

import pytest

from graphscope import nx


@pytest.mark.usefixtures("graphscope_session")
class TestBFS:
    def setup_method(self):
        # simple graph
        G = nx.Graph()
        G.add_edges_from([(0, 1), (1, 2), (1, 3), (2, 4), (3, 4)])
        self.G = G

    def teardown_method(self):
        del self.G

    def test_run_bfs_edges(self):
        nx.builtin.bfs_edges(self.G, source=0, depth_limit=10)

    def test_run_bfs_predecessors(self):
        nx.builtin.bfs_predecessors(self.G, source=0, depth_limit=10)

    def test_run_bfs_successors(self):
        nx.builtin.bfs_successors(self.G, source=0, depth_limit=10)

    @pytest.mark.skip(reason="FIXME(acezen): successor not work on distributed")
    def test_successor(self):
        ctx = nx.builtin.bfs_successors(self.G, source=0, depth_limit=10)
        nd_array = ctx.to_numpy("r", axis=0).tolist()
        assert sorted(nd_array) == [[0, 1], [1, 2], [1, 3], [2, 4]]
        adj_list = (
            ctx.to_dataframe({"r": "r"}).groupby("Col 0")["Col 1"].apply(list).to_dict()
        )
        adj_list_sorted = {}
        for key, value in adj_list.items():
            adj_list_sorted[key] = sorted(value)
        assert adj_list_sorted == {0: [1], 1: [2, 3], 2: [4]}

    def test_predecessor(self):
        ctx = nx.builtin.bfs_predecessors(self.G, source=0, depth_limit=10)
        adj_list = (
            ctx.to_dataframe({"r": "r"}).groupby("Col 0")["Col 1"].apply(list).to_dict()
        )
        assert adj_list == {1: [0], 2: [1], 3: [1], 4: [2]}

    def test_bfs_tree(self):
        T = nx.builtin.bfs_tree(self.G, source=0, depth_limit=10)
        assert sorted(T.nodes()) == sorted(self.G.nodes())
        assert sorted(T.edges()) == [(0, 1), (1, 2), (1, 3), (2, 4)]

    def test_bfs_edges(self):
        edges = nx.builtin.bfs_edges(self.G, source=0, depth_limit=10)
        assert sorted(edges) == [[0, 1], [1, 2], [1, 3], [2, 4]]

    @pytest.mark.skip(reason="tensor context not support empty result.")
    def test_bfs_tree_isolates(self):
        G = nx.Graph()
        G.add_node(1)
        G.add_node(2)
        T = nx.builtin.bfs_tree(G, source=1, depth_limit=10)
        assert sorted(T.nodes()) == [1]
        assert sorted(T.edges()) == []


@pytest.mark.usefixtures("graphscope_session")
class TestBreadthLimitedSearch:
    def setup_method(self):
        # a tree
        G = nx.Graph()
        nx.add_path(G, [0, 1, 2, 3, 4, 5, 6])
        nx.add_path(G, [2, 7, 8, 9, 10])
        self.G = G
        # a disconnected graph
        D = nx.Graph()
        D.add_edges_from([(0, 1), (2, 3)])
        nx.add_path(D, [2, 7, 8, 9, 10])
        self.D = D

    def teardown_method(self):
        del self.G
        del self.D

    def bfs_test_successor(self):
        assert dict(nx.builtin.bfs_successors(self.G, source=1, depth_limit=3)) == {
            1: [0, 2],
            2: [3, 7],
            3: [4],
            7: [8],
        }
        result = {
            n: sorted(s)
            for n, s in nx.builtin.bfs_successors(self.D, source=7, depth_limit=2)
        }
        assert result == {8: [9], 2: [3], 7: [2, 8]}

    def bfs_test_predecessor(self):
        assert dict(nx.builtin.bfs_predecessors(self.G, source=1, depth_limit=3)) == {
            0: 1,
            2: 1,
            3: 2,
            4: 3,
            7: 2,
            8: 7,
        }
        assert dict(nx.builtin.bfs_predecessors(self.D, source=7, depth_limit=2)) == {
            2: 7,
            3: 2,
            8: 7,
            9: 8,
        }

    def bfs_test_tree(self):
        T = nx.builtin.bfs_tree(self.G, source=3, depth_limit=1)
        assert sorted(T.edges()) == [(3, 2), (3, 4)]

    def bfs_test_edges(self):
        edges = nx.builtin.bfs_edges(self.G, source=9, depth_limit=4)
        assert list(edges) == [(9, 8), (9, 10), (8, 7), (7, 2), (2, 1), (2, 3)]
