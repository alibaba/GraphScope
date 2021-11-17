import pytest
from networkx.generators.tests.test_atlas import TestAtlasGraph
from networkx.generators.tests.test_atlas import TestAtlasGraphG

import graphscope.nx as nx
from graphscope.nx import graph_atlas
from graphscope.nx import graph_atlas_g
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestAtlasGraph)
class TestAtlasGraph:
    pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestAtlasGraphG)
class TestAtlasGraphG:
    pass
