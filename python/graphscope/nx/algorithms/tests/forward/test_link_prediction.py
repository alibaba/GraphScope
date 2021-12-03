import networkx.algorithms.tests.test_link_prediction
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_link_prediction,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tests.test_link_prediction import TestAdamicAdarIndex
from networkx.algorithms.tests.test_link_prediction import TestCNSoundarajanHopcroft
from networkx.algorithms.tests.test_link_prediction import TestJaccardCoefficient
from networkx.algorithms.tests.test_link_prediction import TestPreferentialAttachment
from networkx.algorithms.tests.test_link_prediction import \
    TestRAIndexSoundarajanHopcroft
from networkx.algorithms.tests.test_link_prediction import TestResourceAllocationIndex
from networkx.algorithms.tests.test_link_prediction import TestWithinInterCluster


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestResourceAllocationIndex)
class TestResourceAllocationIndex:
    def test_notimplemented(self):
        assert pytest.raises(nx.NetworkXNotImplemented, self.func,
                             nx.DiGraph([(0, 1), (1, 2)]), [(0, 2)])


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestJaccardCoefficient)
class TestJaccardCoefficient:
    def test_notimplemented(self):
        assert pytest.raises(nx.NetworkXNotImplemented, self.func,
                             nx.DiGraph([(0, 1), (1, 2)]), [(0, 2)])


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestPreferentialAttachment)
class TestPreferentialAttachment:
    def test_notimplemented(self):
        assert pytest.raises(nx.NetworkXNotImplemented, self.func,
                             nx.DiGraph([(0, 1), (1, 2)]), [(0, 2)])


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestAdamicAdarIndex)
class TestAdamicAdarIndex:
    def test_notimplemented(self):
        assert pytest.raises(nx.NetworkXNotImplemented, self.func,
                             nx.DiGraph([(0, 1), (1, 2)]), [(0, 2)])


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestCNSoundarajanHopcroft)
class TestCNSoundarajanHopcroft:
    def test_notimplemented(self):
        G = nx.DiGraph([(0, 1), (1, 2)])
        G.add_nodes_from([0, 1, 2], community=0)
        assert pytest.raises(nx.NetworkXNotImplemented, self.func, G, [(0, 2)])


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestRAIndexSoundarajanHopcroft)
class TestRAIndexSoundarajanHopcroft:
    def test_notimplemented(self):
        G = nx.DiGraph([(0, 1), (1, 2)])
        G.add_nodes_from([0, 1, 2], community=0)
        assert pytest.raises(nx.NetworkXNotImplemented, self.func, G, [(0, 2)])


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestWithinInterCluster)
class TestWithinInterCluster:
    def test_notimplemented(self):
        G = nx.DiGraph([(0, 1), (1, 2)])
        G.add_nodes_from([0, 1, 2], community=0)
        assert pytest.raises(nx.NetworkXNotImplemented, self.func, G, [(0, 2)])
