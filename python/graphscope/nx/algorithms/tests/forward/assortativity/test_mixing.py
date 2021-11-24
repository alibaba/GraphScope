import networkx.algorithms.assortativity.tests.test_mixing
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.assortativity.tests.test_mixing,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.assortativity.tests.test_mixing import TestAttributeMixingDict
from networkx.algorithms.assortativity.tests.test_mixing import TestDegreeMixingDict
from networkx.algorithms.assortativity.tests.test_mixing import TestDegreeMixingMatrix

from .base_test import BaseTestAttributeMixing
from .base_test import BaseTestDegreeMixing


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestDegreeMixingDict)
class TestDegreeMixingDict(BaseTestDegreeMixing):
    @pytest.mark.skip(reason="not support multigraph")
    def test_degree_mixing_dict_multigraph(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestDegreeMixingMatrix)
class TestDegreeMixingMatrix(BaseTestDegreeMixing):
    @pytest.mark.skip(reason="not support multigraph")
    def test_degree_mixing_matrix_multigraph(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestAttributeMixingDict)
class TestAttributeMixingDict(BaseTestAttributeMixing):
    @pytest.mark.skip(reason="not support multigraph")
    def test_attribute_mixing_dict_multigraph(self):
        pass
