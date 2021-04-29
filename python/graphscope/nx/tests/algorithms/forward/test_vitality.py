import networkx.algorithms.tests.test_vitality
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_vitality,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tests.test_vitality import TestClosenessVitality


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestClosenessVitality)
class TestClosenessVitality:
    @pytest.mark.skip(reason="not support multigraph")
    def test_weighted_multidigraph(self):
        pass
