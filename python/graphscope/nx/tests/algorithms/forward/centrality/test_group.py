import networkx.algorithms.centrality.tests.test_group
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.centrality.tests.test_group,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
