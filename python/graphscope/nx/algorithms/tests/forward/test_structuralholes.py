import networkx.algorithms.tests.test_structuralholes
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.tests.test_structuralholes,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
