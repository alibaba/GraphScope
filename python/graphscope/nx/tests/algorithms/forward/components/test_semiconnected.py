import networkx.algorithms.components.tests.test_semiconnected
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.components.tests.test_semiconnected,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.components.tests.test_semiconnected import TestIsSemiconnected


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestIsSemiconnected)
class TestIsSemiconnected():
    def test_undirected(self):
        pytest.raises(nx.NetworkXNotImplemented, nx.is_semiconnected, nx.Graph())

    def test_empty(self):
        pytest.raises(nx.NetworkXPointlessConcept, nx.is_semiconnected, nx.DiGraph())
