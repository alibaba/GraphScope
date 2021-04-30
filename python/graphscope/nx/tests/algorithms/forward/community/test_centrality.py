import networkx.algorithms.community.tests.test_centrality
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.community.tests.test_centrality,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
