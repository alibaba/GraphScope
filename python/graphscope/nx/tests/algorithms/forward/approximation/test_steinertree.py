import networkx.algorithms.approximation.tests.test_steinertree
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.approximation.tests.test_steinertree,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.approximation.tests.test_steinertree import TestSteinerTree


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestSteinerTree)
class TestSteinerTree:
    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph_steiner_tree(self):
        pass
