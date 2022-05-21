import networkx.algorithms.link_analysis.tests.test_hits
import networkx.algorithms.link_analysis.tests.test_pagerank
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.link_analysis.tests.test_hits,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

import_as_graphscope_nx(networkx.algorithms.link_analysis.tests.test_pagerank,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestHITS)
class TestHITS:
    @pytest.mark.skip(reason="builtin app not support raise PowerIterationFailedConvergence")
    def test_hits_not_convergent(self):
        pass
