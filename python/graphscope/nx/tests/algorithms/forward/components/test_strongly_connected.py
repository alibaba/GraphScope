import networkx.algorithms.components.tests.test_strongly_connected
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.components.tests.test_strongly_connected,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
