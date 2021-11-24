import networkx.algorithms.tree.tests.test_operations
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.tree.tests.test_operations,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
