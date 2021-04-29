import networkx.algorithms.flow.tests.test_gomory_hu
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.flow.tests.test_gomory_hu,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
