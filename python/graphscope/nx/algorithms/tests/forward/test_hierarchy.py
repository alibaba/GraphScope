import networkx.algorithms.tests.test_hierarchy
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.tests.test_hierarchy,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
