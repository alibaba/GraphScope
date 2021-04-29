import networkx.algorithms.assortativity.tests.test_neighbor_degree
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.assortativity.tests.test_neighbor_degree,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
