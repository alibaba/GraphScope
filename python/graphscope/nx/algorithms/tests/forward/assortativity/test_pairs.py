import networkx.algorithms.assortativity.tests.test_pairs
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.assortativity.tests.test_pairs,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.assortativity.tests.test_pairs import TestAttributeMixingXY
from networkx.algorithms.assortativity.tests.test_pairs import TestDegreeMixingXY

from .base_test import BaseTestAttributeMixing
from .base_test import BaseTestDegreeMixing


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestAttributeMixingXY)
class TestAttributeMixingXY(BaseTestAttributeMixing):
    @pytest.mark.skip(reason="not support multigraph")
    def test_node_attribute_xy_multigraph(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestDegreeMixingXY)
class TestDegreeMixingXY(BaseTestDegreeMixing):
    @pytest.mark.skip(reason="not support multigraph")
    def test_node_degree_xy_multigraph(self):
        pass
