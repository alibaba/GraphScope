import networkx.algorithms.community.tests.test_kclique
import pytest

from graphscope.experimental.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.community.tests.test_kclique,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
