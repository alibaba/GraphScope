import networkx.algorithms.tests.test_distance_regular
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.tests.test_distance_regular,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
