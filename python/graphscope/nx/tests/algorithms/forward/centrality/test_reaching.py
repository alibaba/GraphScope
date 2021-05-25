import networkx.algorithms.centrality.tests.test_reaching
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.centrality.tests.test_reaching,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
