import networkx.algorithms.flow.tests.test_gomory_hu
import networkx.algorithms.flow.tests.test_maxflow
import networkx.algorithms.flow.tests.test_maxflow_large_graph
import networkx.algorithms.flow.tests.test_mincost
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.flow.tests.test_gomory_hu,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.flow.tests.test_maxflow,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.flow.tests.test_maxflow_large_graph,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.flow.tests.test_mincost,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.usefixtures("graphscope_session")
@pytest.mark.slow
@with_graphscope_nx_context(TestGomoryHuTree)
class TestGomoryHuTree:
    pass


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


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestMinCostFlow)
class TestMinCostFlow():
    def test_negative_selfloops(self):
        """Negative selfloops should cause an exception if uncapacitated and
        always be saturated otherwise.
        """
        G = nx.DiGraph()
        G.add_edge(1, 1, weight=-1)
        pytest.raises(nx.NetworkXUnbounded, nx.network_simplex, G)
        pytest.raises(nx.NetworkXUnbounded, nx.capacity_scaling, G)
        G[1][1]['capacity'] = 2
        flowCost, H = nx.network_simplex(G)
        assert flowCost == -2
        assert H == {1: {1: 2}}
        flowCost, H = nx.capacity_scaling(G)
        assert flowCost == -2
        assert H == {1: {1: 2}}

    @pytest.mark.skip(reason="not support value nan")
    def test_exceptions(self):
        G = nx.Graph()
        pytest.raises(nx.NetworkXNotImplemented, nx.network_simplex, G)
        pytest.raises(nx.NetworkXNotImplemented, nx.capacity_scaling, G)
        G = nx.DiGraph()
        pytest.raises(nx.NetworkXError, nx.network_simplex, G)
        pytest.raises(nx.NetworkXError, nx.capacity_scaling, G)
        G.add_node(0, demand=float('inf'))
        pytest.raises(nx.NetworkXError, nx.network_simplex, G)
        pytest.raises(nx.NetworkXUnfeasible, nx.capacity_scaling, G)
        G.nodes[0]['demand'] = 0
        G.add_node(1, demand=0)
        G.add_edge(0, 1, weight=-float('inf'))
        pytest.raises(nx.NetworkXError, nx.network_simplex, G)
        pytest.raises(nx.NetworkXUnfeasible, nx.capacity_scaling, G)
        G[0][1]['weight'] = 0
        G.add_edge(0, 0, weight=float('inf'))
        pytest.raises(nx.NetworkXError, nx.network_simplex, G)
        #pytest.raises(nx.NetworkXError, nx.capacity_scaling, G)
        G[0][0]['weight'] = 0
        G[0][1]['capacity'] = -1
        pytest.raises(nx.NetworkXUnfeasible, nx.network_simplex, G)
        #pytest.raises(nx.NetworkXUnfeasible, nx.capacity_scaling, G)
        G[0][1]['capacity'] = 0
        G[0][0]['capacity'] = -1
        pytest.raises(nx.NetworkXUnfeasible, nx.network_simplex, G)
        #pytest.raises(nx.NetworkXUnfeasible, nx.capacity_scaling, G)

    @pytest.mark.skip(reason="not support read_gpickle")
    def test_large(self):
        pass

    @pytest.mark.skip(reason="not support int as key")
    def test_digraph3(self):
        pass
