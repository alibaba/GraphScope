import networkx.algorithms.shortest_paths.tests.test_astar
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.shortest_paths.tests.test_astar,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.shortest_paths.tests.test_astar import TestAStar


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestAStar)
class TestAStar():
    @pytest.mark.skip(reason="not support multigraph")
    def test_astar_multigraph():
        pass

    @pytest.mark.skip(reason="not support class object as node")
    def test_unorderable_nodes():
        pass
