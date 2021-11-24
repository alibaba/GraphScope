import networkx.algorithms.isomorphism.tests.test_isomorphvf2
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.isomorphism.tests.test_isomorphvf2,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.skip(reason="not support multigraph")
def test_multiedge():
    pass


@pytest.mark.skip(reason="this test class need local test files.")
class TestVF2GraphDB(object):
    pass
