import networkx.algorithms.bipartite.tests.test_basic
import networkx.algorithms.bipartite.tests.test_centrality
import networkx.algorithms.bipartite.tests.test_cluster
import networkx.algorithms.bipartite.tests.test_covering
import networkx.algorithms.bipartite.tests.test_edgelist
import networkx.algorithms.bipartite.tests.test_generators
import networkx.algorithms.bipartite.tests.test_matching
import networkx.algorithms.bipartite.tests.test_matrix
import networkx.algorithms.bipartite.tests.test_project
import networkx.algorithms.bipartite.tests.test_redundancy
import networkx.algorithms.bipartite.tests.test_spectral_bipartivity
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_basic,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_centrality,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_cluster,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_covering,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_edgelist,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_generators,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_matching,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_matrix,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_project,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_redundancy,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_spectral_bipartivity,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestMatching)
class TestMatching():
    @pytest.mark.skip(reason="graphscope.nx not support object as node")
    def test_unorderable_nodes(self):
        pass
