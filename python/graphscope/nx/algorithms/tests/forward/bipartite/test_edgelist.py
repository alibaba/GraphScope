import networkx.algorithms.bipartite.tests.test_edgelist
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_edgelist,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.bipartite.tests.test_edgelist import TestEdgelist


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestEdgelist)
class TestEdgelist():
    @classmethod
    def setup_class(cls):
        cls.G = nx.Graph(name="test")
        e = [('a', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e'), ('e', 'f'), ('a', 'f')]
        cls.G.add_edges_from(e)
        cls.G.add_nodes_from(['a', 'c', 'e'], bipartite=0)
        cls.G.add_nodes_from(['b', 'd', 'f'], bipartite=1)
        cls.G.add_node('g', bipartite=0)
        cls.DG = nx.DiGraph(cls.G)

    @pytest.mark.skip(reason="str(e) not same with networkx")
    def test_write_edgelist_3(self):
        pass

    @pytest.mark.skip(reason="str(e) not same with networkx")
    @pytest.mark.skip(reason="not support multigraph")
    def test_write_edgelist_4(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_edgelist_multigraph(self):
        pass
