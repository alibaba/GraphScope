import networkx.algorithms.bipartite.tests.test_matching
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_matching,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.bipartite.tests.test_matching import TestMatching


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestMatching)
class TestMatching():
    # NB: graphscope.nx does not support tuple node, we remove the disconnecte_graph
    # from setup
    def setup(self):
        """Creates a bipartite graph for use in testing matching algorithms.

        The bipartite graph has a maximum cardinality matching that leaves
        vertex 1 and vertex 10 unmatched. The first six numbers are the left
        vertices and the next six numbers are the right vertices.

        """
        self.simple_graph = nx.complete_bipartite_graph(2, 3)
        self.simple_solution = {0: 2, 1: 3, 2: 0, 3: 1}

        edges = [(0, 7), (0, 8), (2, 6), (2, 9), (3, 8), (4, 8), (4, 9), (5, 11)]
        self.top_nodes = set(range(6))
        self.graph = nx.Graph()
        self.graph.add_nodes_from(range(12))
        self.graph.add_edges_from(edges)

    @pytest.mark.skip(reason="graphscope.nx does not support tuple node")
    def test_eppstein_matching_disconnected(self):
        with pytest.raises(nx.AmbiguousSolution):
            match = eppstein_matching(self.disconnected_graph)

    @pytest.mark.skip(reason="graphscope.nx does not support tuple node")
    def test_hopcroft_karp_matching_disconnected(self):
        with pytest.raises(nx.AmbiguousSolution):
            match = hopcroft_karp_matching(self.disconnected_graph)

    @pytest.mark.skip(reason="graphscope.nx does not support tuple node")
    def test_issue_2127(self):
        pass

    @pytest.mark.skip(reason="graphscope.nx does not support object node")
    def test_unorderable_nodes(self):
        pass
