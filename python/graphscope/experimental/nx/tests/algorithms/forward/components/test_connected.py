import networkx.algorithms.components.tests.test_connected
import pytest

from graphscope.experimental.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.components.tests.test_connected,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
