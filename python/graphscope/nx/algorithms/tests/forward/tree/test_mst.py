import networkx.algorithms.tree.tests.test_mst
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.tree.tests.test_mst,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.skip(reason="not support multigraph")
class MultigraphMSTTestBase():
    pass


@pytest.mark.skip(reason="not support multigraph")
class TestKruskal():
    pass


@pytest.mark.skip(reason="not support multigraph")
class TestPrim():
    pass
