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


@pytest.mark.skip(reason="DynamicFragment duplicated mode not ready.")
@pytest.mark.usefixtures("graphscope_session")
class TestAllSimplePaths:
    """Unit tests for the
    :func:`networkx.algorithms.simple_paths.all_simple_paths` function.
    """

    def test_all_simple_paths(self):
        G = nx.path_graph(4)
        paths = nx.builtin.all_simple_paths(G, 0, 3)
        assert {tuple(p) for p in paths} == {(0, 1, 2, 3)}

    def test_all_simple_paths_with_two_targets_emits_two_paths(self):
        G = nx.path_graph(4)
        G.add_edge(2, 4)
        paths = nx.builtin.all_simple_paths(G, 0, [3, 4])
        assert {tuple(p) for p in paths} == {(0, 1, 2, 3), (0, 1, 2, 4)}

    def test_digraph_all_simple_paths_with_two_targets_emits_two_paths(self):
        G = nx.path_graph(4, create_using=nx.DiGraph())
        G.add_edge(2, 4)
        paths = nx.builtin.all_simple_paths(G, 0, [3, 4])
        assert {tuple(p) for p in paths} == {(0, 1, 2, 3), (0, 1, 2, 4)}

    def test_all_simple_paths_with_two_targets_cutoff(self):
        G = nx.path_graph(4)
        G.add_edge(2, 4)
        paths = nx.builtin.all_simple_paths(G, 0, [3, 4], cutoff=3)
        assert {tuple(p) for p in paths} == {(0, 1, 2, 3), (0, 1, 2, 4)}

    def test_digraph_all_simple_paths_with_two_targets_cutoff(self):
        G = nx.path_graph(4, create_using=nx.DiGraph())
        G.add_edge(2, 4)
        paths = nx.builtin.all_simple_paths(G, 0, [3, 4], cutoff=3)
        assert {tuple(p) for p in paths} == {(0, 1, 2, 3), (0, 1, 2, 4)}

    def test_all_simple_paths_with_two_targets_in_line_emits_two_paths(self):
        G = nx.path_graph(4)
        paths = nx.builtin.all_simple_paths(G, 0, [2, 3])
        assert {tuple(p) for p in paths} == {(0, 1, 2), (0, 1, 2, 3)}

    def test_all_simple_paths_ignores_cycle(self):
        G = nx.cycle_graph(3, create_using=nx.DiGraph())
        G.add_edge(1, 3)
        paths = nx.builtin.all_simple_paths(G, 0, 3)
        assert {tuple(p) for p in paths} == {(0, 1, 3)}

    def test_all_simple_paths_with_two_targets_inside_cycle_emits_two_paths(self):
        G = nx.cycle_graph(3, create_using=nx.DiGraph())
        G.add_edge(1, 3)
        paths = nx.builtin.all_simple_paths(G, 0, [2, 3])
        assert {tuple(p) for p in paths} == {(0, 1, 2), (0, 1, 3)}

    def test_all_simple_paths_source_target(self):
        G = nx.path_graph(4)
        paths = nx.builtin.all_simple_paths(G, 1, 1)
        assert list(paths) == []

    def test_all_simple_paths_cutoff(self):
        G = nx.complete_graph(4)
        paths = nx.builtin.all_simple_paths(G, 0, 1, cutoff=1)
        assert {tuple(p) for p in paths} == {(0, 1)}
        paths = nx.builtin.all_simple_paths(G, 0, 1, cutoff=2)
        assert {tuple(p) for p in paths} == {(0, 1), (0, 2, 1), (0, 3, 1)}

    def test_all_simple_paths_on_non_trivial_graph(self):
        """you may need to draw this graph to make sure it is reasonable"""
        G = nx.path_graph(5, create_using=nx.DiGraph())
        G.add_edges_from([(0, 5), (1, 5), (1, 3), (5, 4), (4, 2), (4, 3)])
        paths = nx.builtin.all_simple_paths(G, 1, [2, 3])
        assert {tuple(p) for p in paths} == {
            (1, 2),
            (1, 3, 4, 2),
            (1, 5, 4, 2),
            (1, 3),
            (1, 2, 3),
            (1, 5, 4, 3),
            (1, 5, 4, 2, 3),
        }
        paths = nx.builtin.all_simple_paths(G, 1, [2, 3], cutoff=3)
        assert {tuple(p) for p in paths} == {
            (1, 2),
            (1, 3, 4, 2),
            (1, 5, 4, 2),
            (1, 3),
            (1, 2, 3),
            (1, 5, 4, 3),
        }
        paths = nx.builtin.all_simple_paths(G, 1, [2, 3], cutoff=2)
        assert {tuple(p) for p in paths} == {(1, 2), (1, 3), (1, 2, 3)}

    @pytest.mark.skip(reason="not support multidigraph")
    def test_all_simple_paths_multigraph(self):
        G = nx.MultiGraph([(1, 2), (1, 2)])
        paths = nx.builtin.all_simple_paths(G, 1, 1)
        assert list(paths) == []
        nx.add_path(G, [3, 1, 10, 2])
        paths = list(nx.builtin.all_simple_paths(G, 1, 2))
        assert len(paths) == 3
        assert {tuple(p) for p in paths} == {(1, 2), (1, 2), (1, 10, 2)}

    @pytest.mark.skip(reason="not support multidigraph")
    def test_all_simple_paths_multigraph_with_cutoff(self):
        G = nx.MultiGraph([(1, 2), (1, 2), (1, 10), (10, 2)])
        paths = list(nx.builtin.all_simple_paths(G, 1, 2, cutoff=1))
        assert len(paths) == 2
        assert {tuple(p) for p in paths} == {(1, 2), (1, 2)}

    def test_all_simple_paths_directed(self):
        G = nx.DiGraph()
        nx.add_path(G, [1, 2, 3])
        nx.add_path(G, [3, 2, 1])
        paths = nx.builtin.all_simple_paths(G, 1, 3)
        assert {tuple(p) for p in paths} == {(1, 2, 3)}

    def test_all_simple_paths_empty(self):
        G = nx.path_graph(4)
        paths = nx.builtin.all_simple_paths(G, 0, 3, cutoff=2)
        assert list(paths) == []

    def test_all_simple_paths_corner_cases(self):
        assert list(nx.builtin.all_simple_paths(nx.empty_graph(2), 0, 0)) == []
        assert list(nx.builtin.all_simple_paths(nx.empty_graph(2), 0, 1)) == []
        assert list(nx.builtin.all_simple_paths(nx.path_graph(9), 0, 8, 0)) == []

    @pytest.mark.skip(reason="not support hamiltonian_path")
    def hamiltonian_path(G, source):
        source = arbitrary_element(G)
        neighbors = set(G[source]) - {source}
        n = len(G)
        for target in neighbors:
            for path in nx.builtin.all_simple_paths(G, source, target):
                if len(path) == n:
                    yield path

    @pytest.mark.skip(reason="not support hamiltonian_path")
    def test_hamiltonian_path(self):
        from itertools import permutations

        G = nx.complete_graph(4)
        paths = [list(p) for p in hamiltonian_path(G, 0)]
        exact = [[0] + list(p) for p in permutations([1, 2, 3], 3)]
        assert sorted(paths) == sorted(exact)

    def test_cutoff_zero(self):
        G = nx.complete_graph(4)
        paths = nx.builtin.all_simple_paths(G, 0, 3, cutoff=0)
        assert list(list(p) for p in paths) == []
        # paths = nx.builtin.all_simple_paths(nx.MultiGraph(G), 0, 3, cutoff=0)
        # assert list(list(p) for p in paths) == []

    def test_source_missing(self):
        with pytest.raises(ValueError, match="nx.NodeNotFound"):
            G = nx.Graph()
            nx.add_path(G, [1, 2, 3])
            list(nx.builtin.all_simple_paths(nx.DiGraph(G), 0, 3))

    def test_target_missing(self):
        with pytest.raises(ValueError, match="nx.NodeNotFound"):
            G = nx.Graph()
            nx.add_path(G, [1, 2, 3])
            list(nx.builtin.all_simple_paths(nx.DiGraph(G), 1, 4))


@pytest.mark.skip(reason="DynamicFragment duplicated mode not ready.")
@pytest.mark.usefixtures("graphscope_session")
class TestAllSimpleEdgePaths:
    """Unit tests for the
    :func:`networkx.algorithms.simple_paths.all_simple_edge_paths` function.
    """

    def test_all_simple_edge_paths(self):
        G = nx.path_graph(4)
        paths = nx.builtin.all_simple_edge_paths(G, 0, 3)
        assert {tuple(p) for p in paths} == {((0, 1), (1, 2), (2, 3))}

    def test_all_simple_edge_paths_with_two_targets_emits_two_paths(self):
        G = nx.path_graph(4)
        G.add_edge(2, 4)
        paths = nx.builtin.all_simple_edge_paths(G, 0, [3, 4])
        assert {tuple(p) for p in paths} == {
            ((0, 1), (1, 2), (2, 3)),
            ((0, 1), (1, 2), (2, 4)),
        }

    def test_digraph_all_simple_edge_paths_with_two_targets_emits_two_paths(self):
        G = nx.path_graph(4, create_using=nx.DiGraph())
        G.add_edge(2, 4)
        paths = nx.builtin.all_simple_edge_paths(G, 0, [3, 4])
        assert {tuple(p) for p in paths} == {
            ((0, 1), (1, 2), (2, 3)),
            ((0, 1), (1, 2), (2, 4)),
        }

    def test_all_simple_edge_paths_with_two_targets_cutoff(self):
        G = nx.path_graph(4)
        G.add_edge(2, 4)
        paths = nx.builtin.all_simple_edge_paths(G, 0, [3, 4], cutoff=3)
        assert {tuple(p) for p in paths} == {
            ((0, 1), (1, 2), (2, 3)),
            ((0, 1), (1, 2), (2, 4)),
        }

    def test_digraph_all_simple_edge_paths_with_two_targets_cutoff(self):
        G = nx.path_graph(4, create_using=nx.DiGraph())
        G.add_edge(2, 4)
        paths = nx.builtin.all_simple_edge_paths(G, 0, [3, 4], cutoff=3)
        assert {tuple(p) for p in paths} == {
            ((0, 1), (1, 2), (2, 3)),
            ((0, 1), (1, 2), (2, 4)),
        }

    def test_all_simple_edge_paths_with_two_targets_in_line_emits_two_paths(self):
        G = nx.path_graph(4)
        paths = nx.builtin.all_simple_edge_paths(G, 0, [2, 3])
        assert {tuple(p) for p in paths} == {((0, 1), (1, 2)), ((0, 1), (1, 2), (2, 3))}

    def test_all_simple_edge_paths_ignores_cycle(self):
        G = nx.cycle_graph(3, create_using=nx.DiGraph())
        G.add_edge(1, 3)
        paths = nx.builtin.all_simple_edge_paths(G, 0, 3)
        assert {tuple(p) for p in paths} == {((0, 1), (1, 3))}

    def test_all_simple_edge_paths_with_two_targets_inside_cycle_emits_two_paths(self):
        G = nx.cycle_graph(3, create_using=nx.DiGraph())
        G.add_edge(1, 3)
        paths = nx.builtin.all_simple_edge_paths(G, 0, [2, 3])
        assert {tuple(p) for p in paths} == {((0, 1), (1, 2)), ((0, 1), (1, 3))}

    def test_all_simple_edge_paths_source_target(self):
        G = nx.path_graph(4)
        paths = nx.builtin.all_simple_edge_paths(G, 1, 1)
        assert list(paths) == []

    def test_all_simple_edge_paths_cutoff(self):
        G = nx.complete_graph(4)
        paths = nx.builtin.all_simple_edge_paths(G, 0, 1, cutoff=1)
        assert {tuple(p) for p in paths} == {((0, 1),)}
        paths = nx.builtin.all_simple_edge_paths(G, 0, 1, cutoff=2)
        assert {tuple(p) for p in paths} == {
            ((0, 1),),
            ((0, 2), (2, 1)),
            ((0, 3), (3, 1)),
        }

    def test_all_simple_edge_paths_on_non_trivial_graph(self):
        """you may need to draw this graph to make sure it is reasonable"""
        G = nx.path_graph(5, create_using=nx.DiGraph())
        G.add_edges_from([(0, 5), (1, 5), (1, 3), (5, 4), (4, 2), (4, 3)])
        paths = nx.builtin.all_simple_edge_paths(G, 1, [2, 3])
        assert {tuple(p) for p in paths} == {
            ((1, 2),),
            ((1, 3), (3, 4), (4, 2)),
            ((1, 5), (5, 4), (4, 2)),
            ((1, 3),),
            ((1, 2), (2, 3)),
            ((1, 5), (5, 4), (4, 3)),
            ((1, 5), (5, 4), (4, 2), (2, 3)),
        }
        paths = nx.builtin.all_simple_edge_paths(G, 1, [2, 3], cutoff=3)
        assert {tuple(p) for p in paths} == {
            ((1, 2),),
            ((1, 3), (3, 4), (4, 2)),
            ((1, 5), (5, 4), (4, 2)),
            ((1, 3),),
            ((1, 2), (2, 3)),
            ((1, 5), (5, 4), (4, 3)),
        }
        paths = nx.builtin.all_simple_edge_paths(G, 1, [2, 3], cutoff=2)
        assert {tuple(p) for p in paths} == {((1, 2),), ((1, 3),), ((1, 2), (2, 3))}

    @pytest.mark.skip(reason="not support multidigraph")
    def test_all_simple_edge_paths_multigraph(self):
        G = nx.MultiGraph([(1, 2), (1, 2)])
        paths = nx.builtin.all_simple_edge_paths(G, 1, 1)
        assert list(paths) == []
        nx.add_path(G, [3, 1, 10, 2])
        paths = list(nx.builtin.all_simple_edge_paths(G, 1, 2))
        assert len(paths) == 3
        assert {tuple(p) for p in paths} == {
            ((1, 2, 0),),
            ((1, 2, 1),),
            ((1, 10, 0), (10, 2, 0)),
        }

    @pytest.mark.skip(reason="not support multidigraph")
    def test_all_simple_edge_paths_multigraph_with_cutoff(self):
        G = nx.MultiGraph([(1, 2), (1, 2), (1, 10), (10, 2)])
        paths = list(nx.builtin.all_simple_edge_paths(G, 1, 2, cutoff=1))
        assert len(paths) == 2
        assert {tuple(p) for p in paths} == {((1, 2, 0),), ((1, 2, 1),)}

    def test_all_simple_edge_paths_directed(self):
        G = nx.DiGraph()
        nx.add_path(G, [1, 2, 3])
        nx.add_path(G, [3, 2, 1])
        paths = nx.builtin.all_simple_edge_paths(G, 1, 3)
        assert {tuple(p) for p in paths} == {((1, 2), (2, 3))}

    def test_all_simple_edge_paths_empty(self):
        G = nx.path_graph(4)
        paths = nx.builtin.all_simple_edge_paths(G, 0, 3, cutoff=2)
        assert list(paths) == []

    def test_all_simple_edge_paths_corner_cases(self):
        assert list(nx.builtin.all_simple_edge_paths(nx.empty_graph(2), 0, 0)) == []
        assert list(nx.builtin.all_simple_edge_paths(nx.empty_graph(2), 0, 1)) == []
        assert list(nx.builtin.all_simple_edge_paths(nx.path_graph(9), 0, 8, 0)) == []

    @pytest.mark.skip(reason="not support hamiltonian_path")
    def hamiltonian_edge_path(G, source):
        source = arbitrary_element(G)
        neighbors = set(G[source]) - {source}
        n = len(G)
        for target in neighbors:
            for path in nx.builtin.all_simple_edge_paths(G, source, target):
                if len(path) == n - 1:
                    yield path

    @pytest.mark.skip(reason="not support hamiltonian_path")
    def test_hamiltonian__edge_path(self):
        from itertools import permutations

        G = nx.complete_graph(4)
        paths = hamiltonian_edge_path(G, 0)
        exact = [list(pairwise([0] + list(p))) for p in permutations([1, 2, 3], 3)]
        assert sorted(exact) == [p for p in sorted(paths)]

    def test_edge_cutoff_zero(self):
        G = nx.complete_graph(4)
        paths = nx.builtin.all_simple_edge_paths(G, 0, 3, cutoff=0)
        assert list(list(p) for p in paths) == []
        # paths = nx.all_simple_edge_paths(nx.MultiGraph(G), 0, 3, cutoff=0)
        # assert list(list(p) for p in paths) == []

    def test_edge_source_missing(self):
        with pytest.raises(ValueError, match="nx.NodeNotFound"):
            G = nx.Graph()
            nx.add_path(G, [1, 2, 3])
            list(nx.builtin.all_simple_edge_paths(nx.DiGraph(G), 0, 3))

    def test_edge_target_missing(self):
        with pytest.raises(ValueError, match="nx.NodeNotFound"):
            G = nx.Graph()
            nx.add_path(G, [1, 2, 3])
            list(nx.builtin.all_simple_edge_paths(nx.DiGraph(G), 1, 4))
