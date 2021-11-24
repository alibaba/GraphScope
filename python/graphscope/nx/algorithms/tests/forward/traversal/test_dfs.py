import networkx.algorithms.traversal.tests.test_dfs
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.traversal.tests.test_dfs,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
