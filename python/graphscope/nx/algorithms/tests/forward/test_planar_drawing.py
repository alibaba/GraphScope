#
# This file is referred and derived from project NetworkX,
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
import networkx.algorithms.tests.test_planar_drawing
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.tests.test_planar_drawing,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


class Vector(object):
    """Compare vectors by their angle without loss of precision

    All vectors in direction [0, 1] are the smallest.
    The vectors grow in clockwise direction.
    """
    __slots__ = ['x', 'y', 'node', 'quadrant']

    def __init__(self, x, y, node):
        self.x = x
        self.y = y
        self.node = node
        if self.x >= 0 and self.y > 0:
            self.quadrant = 1
        elif self.x > 0 and self.y <= 0:
            self.quadrant = 2
        elif self.x <= 0 and self.y < 0:
            self.quadrant = 3
        else:
            self.quadrant = 4

    def __eq__(self, other):
        return (self.quadrant == other.quadrant
                and self.x * other.y == self.y * other.x)

    def __lt__(self, other):
        if self.quadrant < other.quadrant:
            return True
        if self.quadrant > other.quadrant:
            return False
        return self.x * other.y < self.y * other.x

    def __ne__(self, other):
        return not self == other

    def __le__(self, other):
        return not other < self

    def __gt__(self, other):
        return other < self

    def __ge__(self, other):
        return not self < other


def planar_drawing_conforms_to_embedding(embedding, pos):
    """Checks if pos conforms to the planar embedding

    Returns true iff the neighbors are actually oriented in the orientation
    specified of the embedding
    """
    for v in embedding:
        nbr_vectors = []
        v_pos = pos[v]
        for nbr in embedding[v]:
            new_vector = Vector(pos[nbr][0] - v_pos[0], pos[nbr][1] - v_pos[1], nbr)
            nbr_vectors.append(new_vector)
        # Sort neighbors according to their phi angle
        nbr_vectors.sort()
        for idx, nbr_vector in enumerate(nbr_vectors):
            cw_vector = nbr_vectors[(idx + 1) % len(nbr_vectors)]
            ccw_vector = nbr_vectors[idx - 1]
            if (embedding[v][nbr_vector.node]['cw'] != cw_vector.node
                    or embedding[v][nbr_vector.node]['ccw'] != ccw_vector.node):
                return False
            if cw_vector.node != nbr_vector.node and cw_vector == nbr_vector:
                # Lines overlap
                return False
            if ccw_vector.node != nbr_vector.node and ccw_vector == nbr_vector:
                # Lines overlap
                return False
    return True


pytest.mark.usefixtures("graphscope_session")
def check_embedding_data(embedding_data):
    """Checks that the planar embedding of the input is correct"""
    embedding = nx.PlanarEmbedding()
    embedding.set_data(embedding_data)
    pos_fully = nx.combinatorial_embedding_to_pos(embedding, False)
    msg = "Planar drawing does not conform to the embedding (fully " \
          "triangulation)"
    assert planar_drawing_conforms_to_embedding(embedding, pos_fully), msg
    check_edge_intersections(embedding, pos_fully)
    pos_internally = nx.combinatorial_embedding_to_pos(embedding, True)
    msg = "Planar drawing does not conform to the embedding (internal " \
          "triangulation)"
    assert planar_drawing_conforms_to_embedding(embedding, pos_internally), msg
    check_edge_intersections(embedding, pos_internally)


@pytest.mark.usefixtures("graphscope_session")
def test_graph1():
    embedding_data = {0: [1, 2, 3], 1: [2, 0], 2: [3, 0, 1], 3: [2, 0]}
    check_embedding_data(embedding_data)


@pytest.mark.usefixtures("graphscope_session")
def test_graph2():
    embedding_data = {
        0: [8, 6],
        1: [2, 6, 9],
        2: [8, 1, 7, 9, 6, 4],
        3: [9],
        4: [2],
        5: [6, 8],
        6: [9, 1, 0, 5, 2],
        7: [9, 2],
        8: [0, 2, 5],
        9: [1, 6, 2, 7, 3]
    }
    check_embedding_data(embedding_data)


@pytest.mark.usefixtures("graphscope_session")
def test_circle_graph():
    embedding_data = {
        0: [1, 9],
        1: [0, 2],
        2: [1, 3],
        3: [2, 4],
        4: [3, 5],
        5: [4, 6],
        6: [5, 7],
        7: [6, 8],
        8: [7, 9],
        9: [8, 0]
    }
    check_embedding_data(embedding_data)


@pytest.mark.usefixtures("graphscope_session")
def test_grid_graph():
    embedding_data = {
        (0, 1): [(0, 0), (1, 1), (0, 2)],
        (1, 2): [(1, 1), (2, 2), (0, 2)],
        (0, 0): [(0, 1), (1, 0)],
        (2, 1): [(2, 0), (2, 2), (1, 1)],
        (1, 1): [(2, 1), (1, 2), (0, 1), (1, 0)],
        (2, 0): [(1, 0), (2, 1)],
        (2, 2): [(1, 2), (2, 1)],
        (1, 0): [(0, 0), (2, 0), (1, 1)],
        (0, 2): [(1, 2), (0, 1)]
    }
    check_embedding_data(embedding_data)


@pytest.mark.usefixtures("graphscope_session")
def test_two_node_graph():
    embedding_data = {0: [1], 1: [0]}
    check_embedding_data(embedding_data)


@pytest.mark.usefixtures("graphscope_session")
def test_three_node_graph():
    embedding_data = {0: [1, 2], 1: [0, 2], 2: [0, 1]}
    check_embedding_data(embedding_data)


@pytest.mark.usefixtures("graphscope_session")
def test_multiple_component_graph2():
    embedding_data = {0: [1, 2], 1: [0, 2], 2: [0, 1], 3: [4, 5], 4: [3, 5], 5: [3, 4]}
    check_embedding_data(embedding_data)
