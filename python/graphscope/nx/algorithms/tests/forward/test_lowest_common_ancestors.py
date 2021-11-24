import networkx.algorithms.tests.test_lowest_common_ancestors
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_lowest_common_ancestors,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tests.test_lowest_common_ancestors import TestDAGLCA
from networkx.algorithms.tests.test_lowest_common_ancestors import TestTreeLCA


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestDAGLCA)
class TestDAGLCA:
    @pytest.mark.skip(reason="not support None object as node")
    def test_all_pairs_lowest_common_ancestor10(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestTreeLCA)
class TestTreeLCA:
    @pytest.mark.skip(reason="not support None object as node")
    def test_tree_all_pairs_lowest_common_ancestor11(self):
        pass

    def test_not_implemented_for(self):
        NNI = nx.NetworkXNotImplemented
        G = nx.Graph([(0, 1)])
        pytest.raises(NNI, tree_all_pairs_lca, G)
        pytest.raises(NNI, all_pairs_lca, G)
        pytest.raises(NNI, nx.lowest_common_ancestor, G, 0, 1)
