import networkx.algorithms.approximation.tests.test_ramsey
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.approximation.tests.test_ramsey,
                        decorators=pytest.mark.usefixtures("graphscope_session"))
