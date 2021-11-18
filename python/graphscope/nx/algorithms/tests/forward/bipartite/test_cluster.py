import networkx.algorithms.bipartite.tests.test_cluster
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_cluster,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
