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
    @classmethod
    def setup_class(cls):
        cls.DG = nx.DiGraph()
        nx.add_path(cls.DG, (0, 1, 2, 3))
        nx.add_path(cls.DG, (0, 4, 3))
        nx.add_path(cls.DG, (0, 5, 6, 8, 3))
        nx.add_path(cls.DG, (5, 7, 8))
        cls.DG.add_edge(6, 2)
        cls.DG.add_edge(7, 2)

        cls.root_distance = nx.shortest_path_length(cls.DG, source=0)

        # update with graphscope.nx
        cls.gold = {
            (1, 1): 1,
            (1, 2): 1,
            (1, 3): 0,
            (1, 4): 0,
            (1, 5): 0,
            (1, 6): 0,
            (1, 7): 0,
            (1, 8): 0,
            (2, 2): 2,
            (2, 3): 2,
            (2, 4): 0,
            (2, 5): 5,
            (2, 6): 6,
            (2, 7): 7,
            (2, 8): 7,
            (3, 3): 3,
            (3, 4): 4,
            (3, 5): 5,
            (3, 6): 6,
            (3, 7): 7,
            (3, 8): 8,
            (4, 4): 4,
            (4, 5): 0,
            (4, 6): 0,
            (4, 7): 0,
            (4, 8): 0,
            (5, 5): 5,
            (5, 6): 5,
            (5, 7): 5,
            (5, 8): 5,
            (6, 6): 6,
            (6, 7): 5,
            (6, 8): 6,
            (7, 7): 7,
            (7, 8): 7,
            (8, 8): 8,
        }
        cls.gold.update(((0, n), 0) for n in cls.DG)

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
