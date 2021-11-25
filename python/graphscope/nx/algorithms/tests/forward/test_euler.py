import networkx.algorithms.tests.test_euler
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_euler,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tests.test_euler import TestHasEulerianPath
from networkx.algorithms.tests.test_euler import TestIsEulerian
from networkx.algorithms.tests.test_euler import TestIsSemiEulerian


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestIsEulerian)
class TestIsEulerian:
    # NB: graphscope.nx does not support hypercube_graph(which use tuple as node),
    # remove it
    def test_is_eulerian(self):
        assert nx.is_eulerian(nx.complete_graph(5))
        assert nx.is_eulerian(nx.complete_graph(7))

        assert not nx.is_eulerian(nx.complete_graph(4))
        assert not nx.is_eulerian(nx.complete_graph(6))

        assert not nx.is_eulerian(nx.petersen_graph())
        assert not nx.is_eulerian(nx.path_graph(4))


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestHasEulerianPath)
class TestHasEulerianPath:
    # NB: graphscope.nx does not support hypercube_graph(which use tuple as node),
    # remove it
    def test_has_eulerian_path_cyclic(self):
        # Test graphs with Eulerian cycles return True.
        assert nx.has_eulerian_path(nx.complete_graph(5))
        assert nx.has_eulerian_path(nx.complete_graph(7))


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestIsSemiEulerian)
class TestIsSemiEulerian:
    # NB: graphscope.nx does not support hypercube_graph(which use tuple as node),
    # remove it
    def test_is_semieulerian(self):
        # Test graphs with Eulerian paths but no cycles return True.
        assert nx.is_semieulerian(nx.path_graph(4))
        G = nx.path_graph(6, create_using=nx.DiGraph)
        assert nx.is_semieulerian(G)

        # Test graphs with Eulerian cycles return False.
        assert not nx.is_semieulerian(nx.complete_graph(5))
        assert not nx.is_semieulerian(nx.complete_graph(7))
