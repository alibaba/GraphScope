import networkx.algorithms.tests.test_minors
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.tests.test_minors,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.skip(reason="not support Graph object as node.")
class TestQuotient(object):
    pass


@pytest.mark.skip(reason="not support Graph object as node.")
class TestContraction(object):
    pass
