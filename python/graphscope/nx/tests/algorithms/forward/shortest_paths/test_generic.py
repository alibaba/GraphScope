import networkx.algorithms.shortest_paths.tests.test_generic
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.shortest_paths.tests.test_generic,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
