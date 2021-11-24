import networkx.algorithms.isomorphism.tests.test_match_helpers
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx

import_as_graphscope_nx(networkx.algorithms.isomorphism.tests.test_match_helpers,
                        decorators=pytest.mark.usefixtures("graphscope_session"))


@pytest.mark.skip(reason="not support multigraph")
class TestGenericMultiEdgeMatch():
    pass
