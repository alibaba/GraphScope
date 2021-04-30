import networkx.algorithms.coloring.tests.test_coloring
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.coloring.tests.test_coloring,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
