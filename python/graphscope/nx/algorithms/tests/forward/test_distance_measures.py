import networkx.algorithms.tests.test_distance_measures
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_distance_measures,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tests.test_distance_measures import TestBarycenter
from networkx.algorithms.tests.test_distance_measures import TestResistanceDistance


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestResistanceDistance)
class TestResistanceDistance:
    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph(self):
        pass
