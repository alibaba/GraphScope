import networkx.algorithms.traversal.tests.test_bfs
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.traversal.tests.test_bfs,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
