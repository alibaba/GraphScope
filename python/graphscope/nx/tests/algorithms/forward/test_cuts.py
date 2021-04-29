import networkx.algorithms.tests.test_cuts
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_cuts,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tests.test_cuts import TestCutSize
from networkx.algorithms.tests.test_cuts import TestVolume


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestVolume)
class TestVolume:
    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_multidigraph(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestCutSize)
class TestCutSize:
    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph(self):
        pass
