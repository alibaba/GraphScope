import networkx.algorithms.tree.tests.test_mst
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tree.tests.test_mst,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


from networkx.algorithms.tree.tests.test_mst import MinimumSpanningTreeTestBase


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(MinimumSpanningTreeTestBase)
class TestBoruvka:
    algorithm = "boruvka"

    @pytest.mark.skip(reason="orjson not support nan")
    def test_unicode_name(self):
        pass

    @pytest.mark.skip(reason="orjson not support nan")
    def test_nan_weights(self):
        pass

    @pytest.mark.skip(reason="orjson not support nan")
    def test_nan_weights_order(self):
        pass

@pytest.mark.skip(reason="not support multigraph")
class MultigraphMSTTestBase():
    pass


@pytest.mark.skip(reason="not support multigraph")
class TestKruskal():
    pass


@pytest.mark.skip(reason="not support multigraph")
class TestPrim():
    pass
