import networkx.algorithms.tests.test_dominance
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_dominance,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tests.test_dominance import TestDominanceFrontiers
from networkx.algorithms.tests.test_dominance import TestImmediateDominators


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestDominanceFrontiers)
class TestDominanceFrontiers:
    def test_exceptions(self):
        G = nx.Graph()
        G.add_node(0)
        pytest.raises(nx.NetworkXNotImplemented, nx.dominance_frontiers, G, 0)
        G = nx.DiGraph([[0, 0]])
        pytest.raises(nx.NetworkXError, nx.dominance_frontiers, G, 1)


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestImmediateDominators)
class TestImmediateDominators:
    def test_exceptions(self):
        G = nx.Graph()
        G.add_node(0)
        pytest.raises(nx.NetworkXNotImplemented, nx.immediate_dominators, G, 0)
        G = nx.DiGraph([[0, 0]])
        pytest.raises(nx.NetworkXError, nx.immediate_dominators, G, 1)
