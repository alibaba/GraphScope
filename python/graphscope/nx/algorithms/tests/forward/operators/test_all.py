import networkx.algorithms.operators.tests.test_all
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.operators.tests.test_all,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
