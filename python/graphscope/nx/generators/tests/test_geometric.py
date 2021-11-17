import networkx.generators.tests.test_geometric
import pytest
from networkx.generators.tests.test_geometric import TestNavigableSmallWorldGraph

from graphscope.framework.errors import UnimplementedError
from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(
    networkx.generators.tests.test_geometric,
    decorators=pytest.mark.usefixtures("graphscope_session"),
)


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestNavigableSmallWorldGraph)
class TestNavigableSmallWorldGraph:
    def test_navigable_small_world(self):
        with pytest.raises(UnimplementedError):
            G = nx.navigable_small_world_graph(5, p=1, q=0, seed=42)
