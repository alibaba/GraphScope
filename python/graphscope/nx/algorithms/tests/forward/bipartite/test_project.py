import networkx.algorithms.bipartite.tests.test_project
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_project,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
