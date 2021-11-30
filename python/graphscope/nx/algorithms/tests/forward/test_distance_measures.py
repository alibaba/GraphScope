import networkx.algorithms.tests.test_distance_measures
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_distance_measures,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tests.test_distance_measures import TestDistance
from networkx.generators.lattice import grid_2d_graph


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestDistance)
class TestDistance:
    def setup_method(self):
        # NB: graphscope.nx does not support grid_2d_graph(which use tuple as node)
        # we use a tricky way to replace it.
        H = cnlti(grid_2d_graph(4, 4), first_label=1, ordering="sorted")
        G = nx.Graph(H)
        self.G = G
