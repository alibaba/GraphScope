"""Generators - Small
=====================

Some small graphs
"""


import networkx.generators.tests.test_small
import pytest

from graphscope.framework.errors import UnimplementedError
from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(
    networkx.generators.tests.test_small,
    decorators=pytest.mark.usefixtures("graphscope_session"),
)

from networkx.generators.tests.test_small import TestGeneratorsSmall

@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestGeneratorsSmall)
class TestGeneratorsSmall:
    def test_properties_named_small_graphs(self):
        G = nx.bull_graph()
        assert G.number_of_nodes() == 5
        assert G.number_of_edges() == 5
        assert sorted(d for n, d in G.degree()) == [1, 1, 2, 3, 3]
        assert nx.diameter(G) == 3
        assert nx.radius(G) == 2

        G = nx.chvatal_graph()
        assert G.number_of_nodes() == 12
        assert G.number_of_edges() == 24
        assert list(d for n, d in G.degree()) == 12 * [4]
        assert nx.diameter(G) == 2
        assert nx.radius(G) == 2

        G = nx.cubical_graph()
        assert G.number_of_nodes() == 8
        assert G.number_of_edges() == 12
        assert list(d for n, d in G.degree()) == 8 * [3]
        assert nx.diameter(G) == 3
        assert nx.radius(G) == 3

        G = nx.desargues_graph()
        assert G.number_of_nodes() == 20
        assert G.number_of_edges() == 30
        assert list(d for n, d in G.degree()) == 20 * [3]

        G = nx.diamond_graph()
        assert G.number_of_nodes() == 4
        assert sorted(d for n, d in G.degree()) == [2, 2, 3, 3]
        assert nx.diameter(G) == 2
        assert nx.radius(G) == 1

        G = nx.dodecahedral_graph()
        assert G.number_of_nodes() == 20
        assert G.number_of_edges() == 30
        assert list(d for n, d in G.degree()) == 20 * [3]
        assert nx.diameter(G) == 5
        assert nx.radius(G) == 5

        G = nx.frucht_graph()
        assert G.number_of_nodes() == 12
        assert G.number_of_edges() == 18
        assert list(d for n, d in G.degree()) == 12 * [3]
        assert nx.diameter(G) == 4
        assert nx.radius(G) == 3

        G = nx.heawood_graph()
        assert G.number_of_nodes() == 14
        assert G.number_of_edges() == 21
        assert list(d for n, d in G.degree()) == 14 * [3]
        assert nx.diameter(G) == 3
        assert nx.radius(G) == 3

        G = nx.house_graph()
        assert G.number_of_nodes() == 5
        assert G.number_of_edges() == 6
        assert sorted(d for n, d in G.degree()) == [2, 2, 2, 3, 3]
        assert nx.diameter(G) == 2
        assert nx.radius(G) == 2

        G = nx.house_x_graph()
        assert G.number_of_nodes() == 5
        assert G.number_of_edges() == 8
        assert sorted(d for n, d in G.degree()) == [2, 3, 3, 4, 4]
        assert nx.diameter(G) == 2
        assert nx.radius(G) == 1

        G = nx.icosahedral_graph()
        assert G.number_of_nodes() == 12
        assert G.number_of_edges() == 30
        assert list(d for n, d in G.degree()) == [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5]
        assert nx.diameter(G) == 3
        assert nx.radius(G) == 3

        G = nx.krackhardt_kite_graph()
        assert G.number_of_nodes() == 10
        assert G.number_of_edges() == 18
        assert sorted(d for n, d in G.degree()) == [1, 2, 3, 3, 3, 4, 4, 5, 5, 6]

        G = nx.moebius_kantor_graph()
        assert G.number_of_nodes() == 16
        assert G.number_of_edges() == 24
        assert list(d for n, d in G.degree()) == 16 * [3]
        assert nx.diameter(G) == 4

        G = nx.octahedral_graph()
        assert G.number_of_nodes() == 6
        assert G.number_of_edges() == 12
        assert list(d for n, d in G.degree()) == 6 * [4]
        assert nx.diameter(G) == 2
        assert nx.radius(G) == 2

        G = nx.pappus_graph()
        assert G.number_of_nodes() == 18
        assert G.number_of_edges() == 27
        assert list(d for n, d in G.degree()) == 18 * [3]
        assert nx.diameter(G) == 4

        G = nx.petersen_graph()
        assert G.number_of_nodes() == 10
        assert G.number_of_edges() == 15
        assert list(d for n, d in G.degree()) == 10 * [3]
        assert nx.diameter(G) == 2
        assert nx.radius(G) == 2

        G = nx.sedgewick_maze_graph()
        assert G.number_of_nodes() == 8
        assert G.number_of_edges() == 10
        assert sorted(d for n, d in G.degree()) == [1, 2, 2, 2, 3, 3, 3, 4]

        G = nx.tetrahedral_graph()
        assert G.number_of_nodes() == 4
        assert G.number_of_edges() == 6
        assert list(d for n, d in G.degree()) == [3, 3, 3, 3]
        assert nx.diameter(G) == 1
        assert nx.radius(G) == 1

        G = nx.truncated_cube_graph()
        assert G.number_of_nodes() == 24
        assert G.number_of_edges() == 36
        assert list(d for n, d in G.degree()) == 24 * [3]

        G = nx.truncated_tetrahedron_graph()
        assert G.number_of_nodes() == 12
        assert G.number_of_edges() == 18
        assert list(d for n, d in G.degree()) == 12 * [3]

        G = nx.tutte_graph()
        assert G.number_of_nodes() == 46
        assert G.number_of_edges() == 69
        assert list(d for n, d in G.degree()) == 46 * [3]

        # hoffman_singleton_graph not support since it use tuple as node
        with pytest.raises(UnimplementedError):
            G = nx.hoffman_singleton_graph()
