import networkx.algorithms.tests.test_non_randomness
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.tests.test_non_randomness,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
