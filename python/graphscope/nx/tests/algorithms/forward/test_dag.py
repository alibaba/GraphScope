import networkx.algorithms.tests.test_dag
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_dag,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tests.test_dag import TestDAG
from networkx.algorithms.tests.test_dag import TestDagLongestPath


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestDagLongestPath)
class TestDagLongestPath:
    @pytest.mark.skip(reason="not support class object as node")
    def test_unorderable_nodes(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_multidigraph(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestDAG)
class TestDAG:
    @pytest.mark.skip(reason="not support multigraph")
    def test_all_topological_sorts_3(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_all_topological_sorts_multigraph_1(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_all_topological_sorts_multigraph_2(self):
        pass

    @pytest.mark.skip(reason="not support class object as node")
    def test_lexicographical_topological_sort2(self):
        pass


@pytest.mark.skip(reason="not support None object as attribute")
class TestDagToBranching:
    pass
