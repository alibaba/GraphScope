import networkx.algorithms.tests.test_distance_measures
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_distance_measures,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tests.test_distance_measures import TestDistance
from networkx.generators.lattice import grid_2d_graph
