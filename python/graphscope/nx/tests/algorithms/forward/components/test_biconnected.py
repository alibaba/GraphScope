import networkx.algorithms.components.tests.test_biconnected
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.components.tests.test_biconnected,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
