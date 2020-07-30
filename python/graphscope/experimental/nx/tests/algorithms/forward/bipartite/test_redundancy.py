import networkx.algorithms.bipartite.tests.test_redundancy
import pytest

from graphscope.experimental.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_redundancy,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
