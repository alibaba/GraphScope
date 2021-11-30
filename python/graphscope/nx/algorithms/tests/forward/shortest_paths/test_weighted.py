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
import networkx.algorithms.shortest_paths.tests.test_weighted
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.shortest_paths.tests.test_weighted,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.shortest_paths.tests.test_weighted import \
    TestBellmanFordAndGoldbergRadzik
from networkx.algorithms.shortest_paths.tests.test_weighted import TestJohnsonAlgorithm
from networkx.algorithms.shortest_paths.tests.test_weighted import TestWeightedPath
from networkx.algorithms.shortest_paths.tests.test_weighted import WeightedTestBase
from networkx.generators.lattice import grid_2d_graph


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(WeightedTestBase)
class WeightedTestBase():
    def setup(self):
        """Creates some graphs for use in the unit tests."""
        # NB: graphscope.nx does not support grid_2d_graph(which use tuple as node)
        # we use a tricky way to replace it.
        cnlti = nx.convert_node_labels_to_integers
        grid = cnlti(grid_2d_graph(4, 4), first_label=1, ordering="sorted")
        self.grid = nx.Graph(grid)
        self.cycle = nx.cycle_graph(7)
        self.directed_cycle = nx.cycle_graph(7, create_using=nx.DiGraph())
        self.XG = nx.DiGraph()
        self.XG.add_weighted_edges_from([('s', 'u', 10), ('s', 'x', 5), ('u', 'v', 1),
                                         ('u', 'x', 2), ('v', 'y', 1), ('x', 'u', 3),
                                         ('x', 'v', 5), ('x', 'y', 2), ('y', 's', 7),
                                         ('y', 'v', 6)])
        self.XG2 = nx.DiGraph()
        self.XG2.add_weighted_edges_from([[1, 4, 1], [4, 5, 1], [5, 6, 1], [6, 3, 1],
                                          [1, 3, 50], [1, 2, 100], [2, 3, 100]])

        self.XG3 = nx.Graph()
        self.XG3.add_weighted_edges_from([[0, 1, 2], [1, 2, 12], [2, 3, 1], [3, 4, 5],
                                          [4, 5, 1], [5, 0, 10]])

        self.XG4 = nx.Graph()
        self.XG4.add_weighted_edges_from([[0, 1, 2], [1, 2, 2], [2, 3, 1], [3, 4, 1],
                                          [4, 5, 1], [5, 6, 1], [6, 7, 1], [7, 0, 1]])
        self.G = nx.DiGraph()  # no weights
        self.G.add_edges_from([('s', 'u'), ('s', 'x'), ('u', 'v'), ('u', 'x'),
                               ('v', 'y'), ('x', 'u'), ('x', 'v'), ('x', 'y'),
                               ('y', 's'), ('y', 'v')])


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestWeightedPath)
class TestWeightedPath(WeightedTestBase):
    def test_dijkstra(self):
        (D, P) = nx.single_source_dijkstra(self.XG, 's')
        validate_path(self.XG, 's', 'v', 9, P['v'])
        assert D['v'] == 9

        validate_path(self.XG, 's', 'v', 9,
                      nx.single_source_dijkstra_path(self.XG, 's')['v'])
        assert dict(nx.single_source_dijkstra_path_length(self.XG, 's'))['v'] == 9

        validate_path(self.XG, 's', 'v', 9,
                      nx.single_source_dijkstra(self.XG, 's')[1]['v'])

        GG = self.XG.to_undirected()
        # make sure we get lower weight
        # to_undirected might choose either edge with weight 2 or weight 3
        GG['u']['x']['weight'] = 2
        (D, P) = nx.single_source_dijkstra(GG, 's')
        validate_path(GG, 's', 'v', 8, P['v'])
        assert D['v'] == 8  # uses lower weight of 2 on u<->x edge
        validate_path(GG, 's', 'v', 8, nx.dijkstra_path(GG, 's', 'v'))
        assert nx.dijkstra_path_length(GG, 's', 'v') == 8

        validate_path(self.XG2, 1, 3, 4, nx.dijkstra_path(self.XG2, 1, 3))
        validate_path(self.XG3, 0, 3, 15, nx.dijkstra_path(self.XG3, 0, 3))
        assert nx.dijkstra_path_length(self.XG3, 0, 3) == 15
        validate_path(self.XG4, 0, 2, 4, nx.dijkstra_path(self.XG4, 0, 2))
        assert nx.dijkstra_path_length(self.XG4, 0, 2) == 4
        validate_path(self.G, 's', 'v', 2,
                      nx.single_source_dijkstra(self.G, 's', 'v')[1])
        validate_path(self.G, 's', 'v', 2,
                      nx.single_source_dijkstra(self.G, 's')[1]['v'])

        validate_path(self.G, 's', 'v', 2, nx.dijkstra_path(self.G, 's', 'v'))
        assert nx.dijkstra_path_length(self.G, 's', 'v') == 2

        # NetworkXError: node s not reachable from moon
        pytest.raises(nx.NetworkXNoPath, nx.dijkstra_path, self.G, 's', 'moon')
        pytest.raises(nx.NetworkXNoPath, nx.dijkstra_path_length, self.G, 's', 'moon')

        validate_path(self.cycle, 0, 3, 3, nx.dijkstra_path(self.cycle, 0, 3))
        validate_path(self.cycle, 0, 4, 3, nx.dijkstra_path(self.cycle, 0, 4))

        assert nx.single_source_dijkstra(self.cycle, 0, 0) == (0, [0])

    @pytest.mark.skip(reason="not support multigraph")
    def test_single_source_dijkstra_path_length(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_bidirectional_dijkstra_multigraph(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_dijkstra_pred_distance_multigraph(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestBellmanFordAndGoldbergRadzik)
class TestBellmanFordAndGoldbergRadzik(WeightedTestBase):
    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestJohnsonAlgorithm)
class TestJohnsonAlgorithm(WeightedTestBase):
    def test_graphs(self):
        validate_path(self.XG, 's', 'v', 9, nx.johnson(self.XG)['s']['v'])
        validate_path(self.XG2, 1, 3, 4, nx.johnson(self.XG2)[1][3])
        validate_path(self.XG3, 0, 3, 15, nx.johnson(self.XG3)[0][3])
        validate_path(self.XG4, 0, 2, 4, nx.johnson(self.XG4)[0][2])
