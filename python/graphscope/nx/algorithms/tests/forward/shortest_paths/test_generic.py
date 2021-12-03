import networkx.algorithms.shortest_paths.tests.test_generic
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.shortest_paths.tests.test_generic,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.shortest_paths.tests.test_generic import TestGenericPath
from networkx.generators.lattice import grid_2d_graph

pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestGenericPath)
class TestGenericPath:
    def setup_class(cls):
        from networkx import convert_node_labels_to_integers as cnlti

        # NB: graphscope.nx does not support grid_2d_graph(which use tuple as node)
        # we use a tricky way to replace.
        grid = cnlti(grid_2d_graph(4, 4), first_label=1, ordering="sorted")
        cls.grid = nx.Graph(grid)
        cls.cycle = nx.cycle_graph(7)
        cls.directed_cycle = nx.cycle_graph(7, create_using=nx.DiGraph())
        cls.neg_weights = nx.DiGraph()
        cls.neg_weights.add_edge(0, 1, weight=1)
        cls.neg_weights.add_edge(0, 2, weight=3)
        cls.neg_weights.add_edge(1, 3, weight=1)
        cls.neg_weights.add_edge(2, 3, weight=-2)
