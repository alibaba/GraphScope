import networkx.algorithms.flow.tests.test_maxflow_large_graph
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.flow.tests.test_maxflow_large_graph,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.flow.tests.test_maxflow_large_graph import \
    TestMaxflowLargeGraph


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestMaxflowLargeGraph)
class TestMaxflowLargeGraph():
    @pytest.mark.skip(reason="not support tuple as node")
    def test_pyramid(self):
        pass

    @pytest.mark.skip(reason="not support read_gpickle")
    def test_gl1(self):
        pass

    @pytest.mark.skip(reason="not support read_gpickle")
    def test_gw1(self):
        pass

    @pytest.mark.skip(reason="not support read_gpickle")
    def test_wlm3(self):
        pass

    @pytest.mark.skip(reason="not support read_gpickle")
    def test_preflow_push_global_relabel(self):
        pass
