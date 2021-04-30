import networkx.algorithms.assortativity.tests.test_correlation
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.assortativity.tests.test_correlation,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.assortativity.tests.test_correlation import \
    TestAttributeMixingCorrelation
from networkx.algorithms.assortativity.tests.test_correlation import \
    TestDegreeMixingCorrelation

from .base_test import BaseTestAttributeMixing
from .base_test import BaseTestDegreeMixing


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestDegreeMixingCorrelation)
class TestDegreeMixingCorrelation(BaseTestDegreeMixing):
    @pytest.mark.skip(reason="not support multigraph")
    def test_degree_assortativity_multigraph(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_degree_pearson_assortativity_multigraph(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestAttributeMixingCorrelation)
class TestAttributeMixingCorrelation(BaseTestAttributeMixing):
    @pytest.mark.skip(reason="not support multigraph")
    def test_attribute_assortativity_multigraph(self):
        pass
