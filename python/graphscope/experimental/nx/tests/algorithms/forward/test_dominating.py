import networkx.algorithms.tests.test_dominating
import pytest

from graphscope.experimental.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.tests.test_dominating,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
