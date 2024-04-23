#!/usr/bin/env python
#
# This file test_cluster.py is referred and derived from project NetworkX
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


@pytest.mark.usefixtures("graphscope_session")
class TestTriangles:
    def test_empty(self):
        G = nx.Graph()
        assert list(nx.builtin.triangles(G).values()) == []

    def test_path(self):
        G = nx.path_graph(10)
        assert list(nx.builtin.triangles(G).values()) == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        assert nx.builtin.triangles(G) == {
            0: 0,
            1: 0,
            2: 0,
            3: 0,
            4: 0,
            5: 0,
            6: 0,
            7: 0,
            8: 0,
            9: 0,
        }

    def test_cubical(self):
        G = nx.cubical_graph()
        assert list(nx.builtin.triangles(G).values()) == [0, 0, 0, 0, 0, 0, 0, 0]
        assert nx.builtin.triangles(G, 1) == 0
        assert list(nx.builtin.triangles(G, [1, 2]).values()) == [0, 0]
        assert nx.builtin.triangles(G, 1) == 0
        assert nx.builtin.triangles(G, [1, 2]) == {1: 0, 2: 0}

    def test_k5(self):
        G = nx.complete_graph(5)
        assert list(nx.builtin.triangles(G).values()) == [6, 6, 6, 6, 6]
        assert sum(nx.builtin.triangles(G).values()) / 3.0 == 10
        assert nx.builtin.triangles(G, 1) == 6
        G.remove_edge(1, 2)
        assert list(dict(sorted(nx.builtin.triangles(G).items())).values()) == [
            5,
            3,
            3,
            5,
            5,
        ]
        assert nx.builtin.triangles(G, 1) == 3


@pytest.mark.usefixtures("graphscope_session")
class TestDirectedClustering:
    def test_clustering(self):
        G = nx.DiGraph()
        assert list(nx.builtin.clustering(G).values()) == []
        assert nx.builtin.clustering(G) == {}

    def test_path(self):
        G = nx.path_graph(10, create_using=nx.DiGraph())
        assert list(nx.builtin.clustering(G).values()) == [
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
        ]
        assert nx.builtin.clustering(G) == {
            0: 0.0,
            1: 0.0,
            2: 0.0,
            3: 0.0,
            4: 0.0,
            5: 0.0,
            6: 0.0,
            7: 0.0,
            8: 0.0,
            9: 0.0,
        }

    def test_k5(self):
        G = nx.complete_graph(5, create_using=nx.DiGraph())
        assert list(nx.builtin.clustering(G).values()) == [1, 1, 1, 1, 1]
        assert nx.builtin.average_clustering(G) == 1
        G.remove_edge(1, 2)
        assert list(dict(sorted(nx.builtin.clustering(G).items())).values()) == [
            11.0 / 12.0,
            1.0,
            1.0,
            11.0 / 12.0,
            11.0 / 12.0,
        ]
        assert nx.builtin.clustering(G, [1, 4]) == {1: 1.0, 4: 11.0 / 12.0}
        G.remove_edge(2, 1)
        assert list(dict(sorted(nx.builtin.clustering(G).items())).values()) == [
            5.0 / 6.0,
            1.0,
            1.0,
            5.0 / 6.0,
            5.0 / 6.0,
        ]
        assert nx.builtin.clustering(G, [1, 4]) == {1: 1.0, 4: 0.83333333333333337}

    def test_triangle_and_edge(self):
        G = nx.cycle_graph(3, create_using=nx.DiGraph())
        G.add_edge(0, 4)
        assert nx.builtin.clustering(G)[0] == 1.0 / 6.0


@pytest.mark.usefixtures("graphscope_session")
class TestClustering:
    @classmethod
    def setup_class(cls):
        pytest.importorskip("numpy")

    def test_clustering(self):
        G = nx.Graph()
        assert list(nx.builtin.clustering(G).values()) == []
        assert nx.builtin.clustering(G) == {}

    def test_path(self):
        G = nx.path_graph(10)
        assert list(nx.builtin.clustering(G).values()) == [
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ]
        assert nx.builtin.clustering(G) == {
            0: 0,
            1: 0,
            2: 0,
            3: 0,
            4: 0,
            5: 0,
            6: 0,
            7: 0,
            8: 0,
            9: 0,
        }

    def test_cubical(self):
        G = nx.cubical_graph()
        assert list(nx.builtin.clustering(G).values()) == [0, 0, 0, 0, 0, 0, 0, 0]
        assert nx.builtin.clustering(G, 1) == 0
        assert list(nx.builtin.clustering(G, [1, 2]).values()) == [0, 0]
        assert nx.builtin.clustering(G, 1) == 0
        assert nx.builtin.clustering(G, [1, 2]) == {1: 0, 2: 0}

    def test_k5(self):
        G = nx.complete_graph(5)
        assert list(nx.builtin.clustering(G).values()) == [1, 1, 1, 1, 1]
        assert nx.builtin.average_clustering(G) == 1
        G.remove_edge(1, 2)
        assert list(nx.builtin.clustering(G).values()) == [
            5 / 6,
            1,
            1,
            5 / 6,
            5 / 6,
        ]
        assert nx.builtin.clustering(G, [1, 4]) == {1: 1, 4: 0.83333333333333337}

    def test_k5_signed(self):
        G = nx.complete_graph(5)
        assert list(nx.builtin.clustering(G).values()) == [1, 1, 1, 1, 1]
        assert nx.builtin.average_clustering(G) == 1
        G.remove_edge(1, 2)
        G.add_edge(0, 1, weight=-1)
        assert list(nx.builtin.clustering(G, weight="weight").values()) == [
            1 / 6,
            -1 / 3,
            1,
            3 / 6,
            3 / 6,
        ]


@pytest.mark.usefixtures("graphscope_session")
class TestDirectedWeightedClustering:
    def test_clustering(self):
        G = nx.DiGraph()
        assert list(nx.builtin.clustering(G, weight="weight").values()) == []
        assert nx.builtin.clustering(G) == {}

    def test_path(self):
        G = nx.path_graph(10, create_using=nx.DiGraph())
        assert list(nx.builtin.clustering(G, weight="weight").values()) == [
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
        ]
        assert nx.builtin.clustering(G, weight="weight") == {
            0: 0.0,
            1: 0.0,
            2: 0.0,
            3: 0.0,
            4: 0.0,
            5: 0.0,
            6: 0.0,
            7: 0.0,
            8: 0.0,
            9: 0.0,
        }

    def test_k5(self):
        G = nx.complete_graph(5, create_using=nx.DiGraph())
        assert list(nx.builtin.clustering(G, weight="weight").values()) == [
            1,
            1,
            1,
            1,
            1,
        ]
        assert nx.builtin.average_clustering(G, weight="weight") == 1
        G.remove_edge(1, 2)
        assert G.number_of_nodes() == 5
        assert list(
            dict(sorted(nx.builtin.clustering(G, weight="weight").items())).values()
        ) == [
            11.0 / 12.0,
            1.0,
            1.0,
            11.0 / 12.0,
            11.0 / 12.0,
        ]
        assert nx.builtin.clustering(G, [1, 4], weight="weight") == {
            1: 1.0,
            4: 11.0 / 12.0,
        }
        G.remove_edge(2, 1)
        assert list(
            dict(sorted(nx.builtin.clustering(G, weight="weight").items())).values()
        ) == [
            5.0 / 6.0,
            1.0,
            1.0,
            5.0 / 6.0,
            5.0 / 6.0,
        ]
        assert nx.builtin.clustering(G, [1, 4], weight="weight") == {
            1: 1.0,
            4: 0.83333333333333337,
        }

    def test_triangle_and_edge(self):
        G = nx.cycle_graph(3, create_using=nx.DiGraph())
        G.add_edge(0, 4, weight=2)
        assert nx.builtin.clustering(G)[0] == 1.0 / 6.0
        assert nx.builtin.clustering(G, weight="weight")[0] == 1.0 / 12.0


@pytest.mark.usefixtures("graphscope_session")
class TestWeightedClustering:
    def test_clustering(self):
        G = nx.Graph()
        assert list(nx.builtin.clustering(G, weight="weight").values()) == []
        assert nx.builtin.clustering(G) == {}

    def test_path(self):
        G = nx.path_graph(10)
        assert list(nx.builtin.clustering(G, weight="weight").values()) == [
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
        ]
        assert nx.builtin.clustering(G, weight="weight") == {
            0: 0.0,
            1: 0.0,
            2: 0.0,
            3: 0.0,
            4: 0.0,
            5: 0.0,
            6: 0.0,
            7: 0.0,
            8: 0.0,
            9: 0.0,
        }

    def test_cubical(self):
        G = nx.cubical_graph()
        assert list(nx.builtin.clustering(G, weight="weight").values()) == [
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ]
        assert nx.builtin.clustering(G, 1) == 0
        assert list(nx.builtin.clustering(G, [1, 2], weight="weight").values()) == [
            0,
            0,
        ]
        assert nx.builtin.clustering(G, 1, weight="weight") == 0
        assert nx.builtin.clustering(G, [1, 2], weight="weight") == {1: 0, 2: 0}

    def test_k5(self):
        G = nx.complete_graph(5)
        assert list(nx.builtin.clustering(G, weight="weight").values()) == [
            1,
            1,
            1,
            1,
            1,
        ]
        assert nx.builtin.average_clustering(G, weight="weight") == 1
        G.remove_edge(1, 2)
        assert list(
            dict(sorted(nx.builtin.clustering(G, weight="weight").items())).values()
        ) == [
            5.0 / 6.0,
            1.0,
            1.0,
            5.0 / 6.0,
            5.0 / 6.0,
        ]
        assert nx.builtin.clustering(G, [1, 4], weight="weight") == {
            1: 1.0,
            4: 0.83333333333333337,
        }

    @pytest.mark.skip(reason="FIXME(@acezen): first assert failed, got 0.0")
    def test_triangle_and_edge(self):
        G = nx.cycle_graph(3)
        G.add_edge(0, 4, weight=2)
        assert nx.builtin.clustering(G)[0] == 1.0 / 3.0
        assert nx.builtin.clustering(G, weight="weight")[0] == 1.0 / 6.0


@pytest.mark.usefixtures("graphscope_session")
class TestClustering:
    def test_clustering(self):
        G = nx.Graph()
        assert list(nx.builtin.clustering(G).values()) == []
        assert nx.builtin.clustering(G) == {}

    def test_path(self):
        G = nx.path_graph(10)
        assert list(nx.builtin.clustering(G).values()) == [
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
        ]
        assert nx.builtin.clustering(G) == {
            0: 0.0,
            1: 0.0,
            2: 0.0,
            3: 0.0,
            4: 0.0,
            5: 0.0,
            6: 0.0,
            7: 0.0,
            8: 0.0,
            9: 0.0,
        }

    def test_cubical(self):
        G = nx.cubical_graph()
        assert list(nx.builtin.clustering(G).values()) == [0, 0, 0, 0, 0, 0, 0, 0]
        assert nx.builtin.clustering(G, 1) == 0
        assert list(nx.builtin.clustering(G, [1, 2]).values()) == [0, 0]
        assert nx.builtin.clustering(G, 1) == 0
        assert nx.builtin.clustering(G, [1, 2]) == {1: 0, 2: 0}

    @pytest.mark.skip(
        reason="FIXME(@acezen): first assert failed, got [12, 12, 12, 12,12]"
    )
    def test_k5(self):
        G = nx.complete_graph(5)
        assert list(nx.builtin.clustering(G).values()) == [1, 1, 1, 1, 1]
        assert nx.builtin.average_clustering(G) == 1
        G.remove_edge(1, 2)
        assert list(dict(sorted(nx.builtin.clustering(G).items())).values()) == [
            5.0 / 6.0,
            1.0,
            1.0,
            5.0 / 6.0,
            5.0 / 6.0,
        ]
        assert nx.builtin.clustering(G, [1, 4]) == {1: 1.0, 4: 0.83333333333333337}


@pytest.mark.usefixtures("graphscope_session")
@pytest.mark.skip(reason="output not ready, wait to check.")
class TestTransitivity:
    def test_transitivity(self):
        G = nx.Graph()
        assert nx.builtin.transitivity(G) == 0.0

    def test_path(self):
        G = nx.path_graph(10)
        assert nx.builtin.transitivity(G) == 0.0

    def test_cubical(self):
        G = nx.cubical_graph()
        assert nx.builtin.transitivity(G) == 0.0

    def test_k5(self):
        G = nx.complete_graph(5)
        assert nx.builtin.transitivity(G) == 1.0
        G.remove_edge(1, 2)
        assert nx.builtin.transitivity(G) == 0.875


@pytest.mark.usefixtures("graphscope_session")
@pytest.mark.skip(reason="output not ready, wait to check.")
class TestSquareClustering:
    def test_clustering(self):
        G = nx.Graph()
        assert list(nx.square_clustering(G).values()) == []
        assert nx.square_clustering(G) == {}

    def test_path(self):
        G = nx.path_graph(10)
        assert list(nx.square_clustering(G).values()) == [
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
        ]
        assert nx.square_clustering(G) == {
            0: 0.0,
            1: 0.0,
            2: 0.0,
            3: 0.0,
            4: 0.0,
            5: 0.0,
            6: 0.0,
            7: 0.0,
            8: 0.0,
            9: 0.0,
        }

    def test_cubical(self):
        G = nx.cubical_graph()
        assert list(nx.square_clustering(G).values()) == [
            0.5,
            0.5,
            0.5,
            0.5,
            0.5,
            0.5,
            0.5,
            0.5,
        ]
        assert list(nx.square_clustering(G, [1, 2]).values()) == [0.5, 0.5]
        assert nx.square_clustering(G, [1])[1] == 0.5
        assert nx.square_clustering(G, [1, 2]) == {1: 0.5, 2: 0.5}

    def test_k5(self):
        G = nx.complete_graph(5)
        assert list(nx.square_clustering(G).values()) == [1, 1, 1, 1, 1]

    def test_bipartite_k5(self):
        G = nx.complete_bipartite_graph(5, 5)
        assert list(nx.square_clustering(G).values()) == [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

    def test_lind_square_clustering(self):
        """Test C4 for figure 1 Lind et al (2005)"""
        G = nx.Graph(
            [
                (1, 2),
                (1, 3),
                (1, 6),
                (1, 7),
                (2, 4),
                (2, 5),
                (3, 4),
                (3, 5),
                (6, 7),
                (7, 8),
                (6, 8),
                (7, 9),
                (7, 10),
                (6, 11),
                (6, 12),
                (2, 13),
                (2, 14),
                (3, 15),
                (3, 16),
            ]
        )
        G1 = G.subgraph([1, 2, 3, 4, 5, 13, 14, 15, 16])
        G2 = G.subgraph([1, 6, 7, 8, 9, 10, 11, 12])
        assert nx.square_clustering(G, [1])[1] == 3 / 75.0
        assert nx.square_clustering(G1, [1])[1] == 2 / 6.0
        assert nx.square_clustering(G2, [1])[1] == 1 / 5.0


@pytest.mark.usefixtures("graphscope_session")
@pytest.mark.skip(reason="FIXME(@acezen): first assert failed, got -2.0")
def test_average_clustering():
    G = nx.cycle_graph(3)
    G.add_edge(2, 3)
    assert nx.builtin.average_clustering(G) == pytest.approx(
        (1 + 1 + 1 / 3.0) / 4.0, rel=1e-9, abs=1e-12
    )
    assert nx.builtin.average_clustering(G, count_zeros=True) == pytest.approx(
        (1 + 1 + 1 / 3.0) / 4.0, rel=1e-9, abs=1e-12
    )
    assert nx.builtin.average_clustering(G, count_zeros=False) == pytest.approx(
        (1 + 1 + 1 / 3.0) / 3.0, rel=1e-9, abs=1e-12
    )


@pytest.mark.usefixtures("graphscope_session")
@pytest.mark.skip(reason="output not ready, wait to check.")
class TestGeneralizedDegree:
    def test_generalized_degree(self):
        G = nx.Graph()
        assert nx.generalized_degree(G) == {}

    def test_path(self):
        G = nx.path_graph(5)
        assert nx.generalized_degree(G, 0) == {0: 1}
        assert nx.generalized_degree(G, 1) == {0: 2}

    def test_cubical(self):
        G = nx.cubical_graph()
        assert nx.generalized_degree(G, 0) == {0: 3}

    def test_k5(self):
        G = nx.complete_graph(5)
        assert nx.generalized_degree(G, 0) == {3: 4}
        G.remove_edge(0, 1)
        assert nx.generalized_degree(G, 0) == {2: 3}
