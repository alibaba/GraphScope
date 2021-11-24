import networkx.algorithms.flow.tests.test_maxflow
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.flow.tests.test_maxflow,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.flow.tests.test_maxflow import TestCutoff
from networkx.algorithms.flow.tests.test_maxflow import TestMaxflowMinCutCommon


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestMaxflowMinCutCommon)
class TestMaxflowMinCutCommon():
    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraphs_raise(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestCutoff)
class TestCutoff():
    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraphs_raise(self):
        pass

    @pytest.mark.skip(reason="not support tuple as node")
    def test_cutoff(self):
        k = 5
        p = 1000
        G = nx.DiGraph()
        for i in range(k):
            G.add_edge('s', (i, 0), capacity=2)
            nx.add_path(G, ((i, j) for j in range(p)), capacity=2)
            G.add_edge((i, p - 1), 't', capacity=2)
        R = shortest_augmenting_path(G, 's', 't', two_phase=True, cutoff=k)
        assert k <= R.graph['flow_value'] <= (2 * k)
        R = shortest_augmenting_path(G, 's', 't', two_phase=False, cutoff=k)
        assert k <= R.graph['flow_value'] <= (2 * k)
        R = edmonds_karp(G, 's', 't', cutoff=k)
        assert k <= R.graph['flow_value'] <= (2 * k)


@pytest.mark.skip(reason="not support tuple as node")
def test_shortest_augmenting_path_two_phase():
    k = 5
    p = 1000
    G = nx.DiGraph()
    for i in range(k):
        G.add_edge('s', (i, 0), capacity=1)
        nx.add_path(G, ((i, j) for j in range(p)), capacity=1)
        G.add_edge((i, p - 1), 't', capacity=1)
    R = shortest_augmenting_path(G, 's', 't', two_phase=True)
    assert R.graph['flow_value'] == k
    R = shortest_augmenting_path(G, 's', 't', two_phase=False)
    assert R.graph['flow_value'] == k
