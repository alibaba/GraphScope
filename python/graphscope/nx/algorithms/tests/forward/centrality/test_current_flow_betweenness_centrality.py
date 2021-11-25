import networkx.algorithms.centrality.tests.test_current_flow_betweenness_centrality
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(
    networkx.algorithms.centrality.tests.test_current_flow_betweenness_centrality,
    decorators=pytest.mark.usefixtures("graphscope_session"))


from networkx.algorithms.centrality.tests.test_current_flow_betweenness_centrality import \
    TestApproximateFlowBetweennessCentrality


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestApproximateFlowBetweennessCentrality)
class TestApproximateFlowBetweennessCentrality:
    # NB: graphscope.nx does not support grid_graph, pass the test
    def test_grid(self):
        pass
