import networkx.algorithms.tests.test_euler
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_euler,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tests.test_euler import TestEulerianCircuit
from networkx.algorithms.tests.test_euler import TestEulerize
from networkx.algorithms.tests.test_euler import TestIsEulerian


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestIsEulerian)
class TestIsEulerian:
    def test_is_eulerian2(self):
        # not connected
        G = nx.Graph()
        G.add_nodes_from([1, 2, 3])
        assert not nx.is_eulerian(G)
        # not strongly connected
        G = nx.DiGraph()
        G.add_nodes_from([1, 2, 3])
        assert not nx.is_eulerian(G)


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestEulerianCircuit)
class TestEulerianCircuit:
    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph_with_keys(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestEulerize)
class TestEulerize:
    def test_on_complete_graph(self):
        G = nx.complete_graph(4)
        assert nx.is_eulerian(nx.eulerize(G))

    @pytest.mark.skip(reason="not support multigraph")
    def test_null_multigraph(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_on_eulerian_multigraph(self):
        pass
